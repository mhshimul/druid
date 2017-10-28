/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata.storage.oracle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.client.DruidServer;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataRuleManagerConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ManageLifecycle
public class OracleMetadataRuleManager implements MetadataRuleManager
{


  private static final EmittingLogger log = new EmittingLogger(OracleMetadataRuleManager.class);
  private final ObjectMapper jsonMapper;
  private final MetadataRuleManagerConfig config;
  private final MetadataStorageTablesConfig dbTables;
  private final IDBI dbi;
  private final AtomicReference<ImmutableMap<String, List<Rule>>> rules;
  private final AuditManager auditManager;
  private final Object lock = new Object();
  private volatile boolean started = false;
  private volatile ListeningScheduledExecutorService exec = null;
  private volatile ListenableFuture<?> future = null;
  private volatile long retryStartTime = 0;

  @Inject
  public OracleMetadataRuleManager(
      @Json ObjectMapper jsonMapper,
      MetadataRuleManagerConfig config,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      AuditManager auditManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dbi = connector.getDBI();
    this.auditManager = auditManager;

    // Verify configured Periods can be treated as Durations (fail-fast before they're needed).
    Preconditions.checkNotNull(config.getAlertThreshold().toStandardDuration());
    Preconditions.checkNotNull(config.getPollDuration().toStandardDuration());

    this.rules = new AtomicReference<>(
        ImmutableMap.<String, List<Rule>>of()
    );
  }

  public static void createDefaultRule(
      final IDBI dbi,
      final String ruleTable,
      final String defaultDatasourceName,
      final ObjectMapper jsonMapper
  )
  {
    try {
      dbi.withHandle((HandleCallback<Void>) handle -> {
        List<Map<String, Object>> existing = handle.createQuery(String.format(
            "SELECT id from %s where datasource=:dataSource",
            ruleTable
        )).bind("dataSource", defaultDatasourceName).list();

        if (!existing.isEmpty()) {
          return null;
        }

        final List<Rule> defaultRules = Collections.singletonList(new ForeverLoadRule(ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )));
        final String version = new DateTime().toString();
        handle.createStatement(String.format(
            "INSERT INTO %s (id, dataSource, version, payload) VALUES (:id, :dataSource, :version, :payload)",
            ruleTable
        ))
              .bind("id", String.format("%s_%s", defaultDatasourceName, version))
              .bind("dataSource", defaultDatasourceName)
              .bind("version", version)
              .bind("payload", jsonMapper.writeValueAsBytes(defaultRules))
              .execute();

        return null;
      });
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded("DatabaseRuleManager-Exec--%d"));

      createDefaultRule(dbi, getRulesTable(), config.getDefaultRule(), jsonMapper);
      future = exec.scheduleWithFixedDelay(() -> {
        try {
          poll();
        }
        catch (Exception e) {
          log.error(e, "uncaught exception in rule manager polling thread");
        }
      }, 0, config.getPollDuration().toStandardDuration().getMillis(), TimeUnit.MILLISECONDS);

      started = true;
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      rules.set(ImmutableMap.of());

      future.cancel(false);
      future = null;
      started = false;
      exec.shutdownNow();
      exec = null;
    }
  }

  @Override
  public void poll()
  {
    try {
      ImmutableMap<String, List<Rule>> newRules = ImmutableMap.copyOf(dbi.withHandle(handle -> handle.createQuery(
          // Return latest version rule by dataSource
          String.format("SELECT r.dataSource, r.payload "
                        + "FROM %1$s r "
                        + "INNER JOIN(SELECT dataSource, max(version) as version FROM %1$s GROUP BY dataSource) ds "
                        + "ON r.datasource = ds.datasource and r.version = ds.version", getRulesTable()))
                                                                                                     .map((index, r, ctx) -> {
                                                                                                       try {
                                                                                                         return Pair.of(
                                                                                                             r.getString(
                                                                                                                 "dataSource"),
                                                                                                             jsonMapper.<List<Rule>>readValue(
                                                                                                                 r.getBytes(
                                                                                                                     "payload"),
                                                                                                                 new TypeReference<List<Rule>>()
                                                                                                                 {
                                                                                                                 }
                                                                                                             )
                                                                                                         );
                                                                                                       }
                                                                                                       catch (IOException e) {
                                                                                                         throw Throwables
                                                                                                             .propagate(
                                                                                                                 e);
                                                                                                       }
                                                                                                     })
                                                                                                     .fold(
                                                                                                         Maps.newHashMap(),
                                                                                                         (Folder3<Map<String, List<Rule>>, Pair<String, List<Rule>>>) (retVal, stringObjectMap, foldController, statementContext) -> {
                                                                                                           try {
                                                                                                             String dataSource = stringObjectMap.lhs;
                                                                                                             retVal.put(
                                                                                                                 dataSource,
                                                                                                                 stringObjectMap.rhs
                                                                                                             );
                                                                                                             return retVal;
                                                                                                           }
                                                                                                           catch (Exception e) {
                                                                                                             throw Throwables
                                                                                                                 .propagate(
                                                                                                                     e);
                                                                                                           }
                                                                                                         }
                                                                                                     )
                                                                      )
      );

      log.info("Polled and found rules for %,d datasource(s)", newRules.size());

      rules.set(newRules);
      retryStartTime = 0;
    }
    catch (Exception e) {
      if (retryStartTime == 0) {
        retryStartTime = System.currentTimeMillis();
      }

      if (System.currentTimeMillis() - retryStartTime > config.getAlertThreshold().toStandardDuration().getMillis()) {
        log.makeAlert(e, "Exception while polling for rules")
           .emit();
        retryStartTime = 0;
      } else {
        log.error(e, "Exception while polling for rules");
      }
    }
  }

  @Override
  public Map<String, List<Rule>> getAllRules()
  {
    return rules.get();
  }

  @Override
  public List<Rule> getRules(final String dataSource)
  {
    List<Rule> retVal = rules.get().get(dataSource);
    return retVal == null ? Lists.newArrayList() : retVal;
  }

  @Override
  public List<Rule> getRulesWithDefault(final String dataSource)
  {
    List<Rule> retVal = Lists.newArrayList();
    Map<String, List<Rule>> theRules = rules.get();
    if (theRules.get(dataSource) != null) {
      retVal.addAll(theRules.get(dataSource));
    }
    if (theRules.get(config.getDefaultRule()) != null) {
      retVal.addAll(theRules.get(config.getDefaultRule()));
    }
    return retVal;
  }

  @Override
  public boolean overrideRule(final String dataSource, final List<Rule> newRules, final AuditInfo auditInfo)
  {
    final String ruleString;
    try {
      ruleString = jsonMapper.writeValueAsString(newRules);
      log.info("Updating [%s] with rules [%s] as per [%s]", dataSource, ruleString, auditInfo);
    }
    catch (JsonProcessingException e) {
      log.error(e, "Unable to write rules as string for [%s]", dataSource);
      return false;
    }
    synchronized (lock) {
      try {
        dbi.inTransaction((TransactionCallback<Void>) (handle, transactionStatus) -> {
          final DateTime auditTime = DateTime.now();
          auditManager.doAudit(AuditEntry.builder()
                                         .key(dataSource)
                                         .type("rules")
                                         .auditInfo(auditInfo)
                                         .payload(ruleString)
                                         .auditTime(auditTime)
                                         .build(), handle);
          String version = auditTime.toString();
          handle.createStatement(String.format(
              "INSERT INTO %s (id, dataSource, version, payload) VALUES (:id, :dataSource, :version, :payload)",
              getRulesTable()
          ))
                .bind("id", String.format("%s_%s", dataSource, version))
                .bind("dataSource", dataSource)
                .bind("version", version)
                .bind("payload", jsonMapper.writeValueAsBytes(newRules))
                .execute();

          return null;
        });
      }
      catch (Exception e) {
        log.error(e, String.format("Exception while overriding rule for %s", dataSource));
        return false;
      }
    }
    try {
      poll();
    }
    catch (Exception e) {
      log.error(e, String.format("Exception while polling for rules after overriding the rule for %s", dataSource));
    }
    return true;
  }

  private String getRulesTable()
  {
    return dbTables.getRulesTable();
  }
}
