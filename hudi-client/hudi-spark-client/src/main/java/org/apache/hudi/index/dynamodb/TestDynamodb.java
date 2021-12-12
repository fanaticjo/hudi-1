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

package org.apache.hudi.index.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestDynamodb {
  @SuppressWarnings("checkstyle:WhitespaceAround")
  public static void main(String[] args) {
    TestDynamodb ob = new TestDynamodb();
    List<String> a = new ArrayList<String>();
    a.add("test");
    a.add("d");
    a.add("test");
    a.add("e");
    ob.write();
    Map<String, List<Item>> results = ob.doget(a);
    System.out.println(results.get("test"));
    for (Item result : results.get("test")) {
      System.out.println(result.get("sk"));
    }
  }

  private DynamoDB getConnection() {
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        .withRegion("us-east-1").build();
    return new DynamoDB(client);
  }

  private Map<String, List<Item>> doget(List<String> keys) {
    if (keys.size() > 0) {
      BatchGetItemOutcome outcome = getConnection().batchGetItem(new TableKeysAndAttributes("test").addHashAndRangePrimaryKeys("pk", "sk", keys.toArray(new String[keys.size()])));
      return outcome.getTableItems();
    }
    return null;
  }

  public boolean write() {
    TableWriteItems tale = new TableWriteItems("test")
        .withItemsToPut(
            new Item()
                .withPrimaryKey("pk", "tjhljljljlest", "sk", "aaaaasdadad")
                .withString("roll", "ghf"),
            new Item()
                .withPrimaryKey("pk", "tejljljlst", "sk", "aaaaa")
                .withString("roll", "ghf")
        ).addHashOnlyPrimaryKeysToDelete("pk", "test");
    //TableWriteItems tale2 = new TableWriteItems("test").addHashOnlyPrimaryKeysToDelete("pk", "test");
    BatchWriteItemOutcome outcome = getConnection().batchWriteItem(tale);
    return true;
  }
}
