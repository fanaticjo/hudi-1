/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

@ConfigClassProperty(name = "Dynamodb Index Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations for Dynamodb for insert / upsert"
)
public class HoodieDynamodbConfig extends HoodieConfig {
  public static final ConfigProperty<String> DYNAMODB_TABLE_NAME = ConfigProperty
      .key("hoodie.index.dynamodb.table")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. Dynamodb Table name to use as the index. "
          + "Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table");

  public static final ConfigProperty<String> DYNAMODB_TABLE_REGION = ConfigProperty
      .key("hoodie.dynamodb.region")
      .defaultValue("us-east-1")
      .withDocumentation("Region where dynamodb Table would be there");

  private HoodieDynamodbConfig() {
    super();
  }

  public static HoodieDynamodbConfig.Builder newBuilder() {
    return new HoodieDynamodbConfig.Builder();
  }

  public static class Builder {

    private final HoodieDynamodbConfig hoodieDynamodbConfig = new HoodieDynamodbConfig();

    public HoodieDynamodbConfig.Builder dynamodbTableName(String name) {
      hoodieDynamodbConfig.setValue(DYNAMODB_TABLE_NAME, name);
      return this;
    }

    public HoodieDynamodbConfig.Builder dynamodbTableRegion(String region) {
      hoodieDynamodbConfig.setValue(DYNAMODB_TABLE_REGION, region);
      return this;
    }

    public HoodieDynamodbConfig build() {
      hoodieDynamodbConfig.setDefaults(HoodieDynamodbConfig.class.getName());
      return hoodieDynamodbConfig;
    }

  }

}
