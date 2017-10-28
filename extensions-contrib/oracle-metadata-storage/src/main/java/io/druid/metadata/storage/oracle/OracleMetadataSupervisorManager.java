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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;
import io.druid.indexing.overlord.supervisor.VersionedSupervisorSpec;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.MetadataSupervisorManager;
import io.druid.metadata.SQLMetadataConnector;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@ManageLifecycle
@SuppressWarnings("unchecked")
public class OracleMetadataSupervisorManager implements MetadataSupervisorManager
{
  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final IDBI dbi;

  @Inject
  public OracleMetadataSupervisorManager(
      @Json ObjectMapper jsonMapper,
      SQLMetadataConnector connector,
      Supplier<MetadataStorageTablesConfig> dbTables
  )
  {
    this.jsonMapper = jsonMapper;
    this.connector = connector;
    this.dbTables = dbTables;
    this.dbi = connector.getDBI();
  }

  @LifecycleStart
  public void start()
  {
    this.connector.createSupervisorsTable();
  }

  public void insert(final String id, final SupervisorSpec spec)
  {
    this.dbi.withHandle(handle -> {
      handle.createStatement(String.format(
          "INSERT INTO %s (id, spec_id, created_date, payload) VALUES (druid_supervisors_seq.nextval, :spec_id, :created_date, :payload)",
          OracleMetadataSupervisorManager.this.getSupervisorsTable()
      ))
            .bind("spec_id", id)
            .bind("created_date", (new DateTime()).toString())
            .bind("payload", OracleMetadataSupervisorManager.this.jsonMapper.writeValueAsBytes(spec))
            .execute();
      return null;
    });
  }

  public Map<String, List<VersionedSupervisorSpec>> getAll()
  {
    return ImmutableMap.copyOf((Map) this.dbi.withHandle(handle -> (Map) handle.createQuery(String.format(
        "SELECT id, spec_id, created_date, payload FROM %1$s ORDER BY id DESC",
        OracleMetadataSupervisorManager.this.getSupervisorsTable()
    )).map((index, r, ctx) -> {
      try {
        SupervisorSpec payload = OracleMetadataSupervisorManager.this.jsonMapper.readValue(
            r.getBytes("payload"),
            new TypeReference<SupervisorSpec>()
            {
            }
        );
        return Pair.of(r.getString("spec_id"), new VersionedSupervisorSpec(payload, r.getString("created_date")));
      }
      catch (IOException var5) {
        throw Throwables.propagate(var5);
      }
    }).fold(Maps.newHashMap(), (retVal, pair, foldController, statementContext) -> {
      try {
        String specId = pair.lhs;
        if (!retVal.containsKey(specId)) {
          retVal.put(specId, Lists.newArrayList());
        }

        ((List) retVal.get(specId)).add(pair.rhs);
        return retVal;
      }
      catch (Exception var6) {
        throw Throwables.propagate(var6);
      }
    })));
  }

  public Map<String, SupervisorSpec> getLatest()
  {
    return ImmutableMap.copyOf((Map) this.dbi.withHandle((HandleCallback<Map<String, SupervisorSpec>>) handle -> (Map) handle
        .createQuery(String.format(
            "SELECT r.spec_id, r.payload FROM %1$s r INNER JOIN(SELECT spec_id, max(id) as id FROM %1$s GROUP BY spec_id) latest ON r.id = latest.id",
            OracleMetadataSupervisorManager.this.getSupervisorsTable()
        ))
        .map((ResultSetMapper<Pair<String, SupervisorSpec>>) (index, r, ctx) -> {
          try {
            return Pair.of(
                r.getString("spec_id"),
                OracleMetadataSupervisorManager.this.jsonMapper.readValue(
                    r.getBytes("payload"),
                    new TypeReference<SupervisorSpec>()
                    {
                    }
                )
            );
          }
          catch (IOException var5) {
            throw Throwables.propagate(var5);
          }
        })
        .fold(
            Maps.newHashMap(),
            (Folder3<Map<String, SupervisorSpec>, Pair<String, SupervisorSpec>>) (retVal, stringObjectMap, foldController, statementContext) -> {
              try {
                retVal.put(stringObjectMap.lhs, stringObjectMap.rhs);
                return retVal;
              }
              catch (Exception var6) {
                throw Throwables.propagate(var6);
              }
            }
        )));
  }

  private String getSupervisorsTable()
  {
    return this.dbTables.get().getSupervisorTable();
  }
}
