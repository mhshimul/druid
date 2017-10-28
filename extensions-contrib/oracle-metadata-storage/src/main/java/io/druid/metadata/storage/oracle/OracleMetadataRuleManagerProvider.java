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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.audit.AuditManager;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.metadata.MetadataRuleManagerConfig;
import io.druid.metadata.MetadataRuleManagerProvider;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.IDBI;

public class OracleMetadataRuleManagerProvider implements MetadataRuleManagerProvider
{
  private final ObjectMapper jsonMapper;
  private final MetadataRuleManagerConfig config;
  private final MetadataStorageTablesConfig dbTables;
  private final SQLMetadataConnector connector;
  private final Lifecycle lifecycle;
  private final IDBI dbi;
  private final AuditManager auditManager;

  @Inject
  public OracleMetadataRuleManagerProvider(
      ObjectMapper jsonMapper,
      MetadataRuleManagerConfig config,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      Lifecycle lifecycle,
      OracleAuditManager auditManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.connector = connector;
    this.dbi = connector.getDBI();
    this.lifecycle = lifecycle;
    this.auditManager = auditManager;
  }

  @Override
  public OracleMetadataRuleManager get()
  {
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              connector.createRulesTable();
              OracleMetadataRuleManager.createDefaultRule(
                  dbi, dbTables.getRulesTable(), config.getDefaultRule(), jsonMapper
              );
            }

            @Override
            public void stop()
            {

            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return new OracleMetadataRuleManager(jsonMapper, config, dbTables, connector, auditManager);
  }
}
