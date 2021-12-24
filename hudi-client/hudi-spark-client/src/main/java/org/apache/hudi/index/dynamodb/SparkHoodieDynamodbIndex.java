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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.SparkMemoryUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.hbase.SparkHoodieHBaseIndex;
import org.apache.hudi.table.HoodieTable;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Hoodie Index implementation backed by Dynamodb.
 */
public class SparkHoodieDynamodbIndex<T extends HoodieRecordPayload<T>>
    extends HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final String PRIMARY_KEY = "primary_key";
  private static final String COMMIT_TS_COLUMN = "commit_ts";
  private static final String FILE_NAME_COLUMN = "file_name";
  private static final String PARTITION_PATH_COLUMN = "partition_path";

  private static final Logger LOG = LogManager.getLogger(SparkHoodieDynamodbIndex.class);
  private static DynamoDB dynamodbConnection = null;
  private final String tableName;
  private long totalNumInserts;
  private int numWriteStatusWithInserts;

  public SparkHoodieDynamodbIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = "student";
  }

  private DynamoDB getConnection() {
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        .withRegion("us-east-1").build();
    return new DynamoDB(client);
  }

  private boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty()
        && commitTimeline.containsOrBeforeTimelineStarts(commitTs);
  }

  private Map<String, List<Item>> doget(List<HoodieKey> keys) {
    if (keys.size() > 0) {
      List<String> alternateKeys = new ArrayList<String>();
      LOG.info("Hello Dynamodb Index get operation has started");
      LOG.info(keys);
      keys.forEach(data -> {
        LOG.info("hello checking the keys");
        LOG.info(data.getPartitionPath());
        LOG.info(data.getRecordKey());
        alternateKeys.add(data.getRecordKey());
        alternateKeys.add(data.getPartitionPath());
      });
      LOG.info("Here i am please check the alternate keys");
      LOG.info(alternateKeys);
      BatchGetItemOutcome outcome = dynamodbConnection
          .batchGetItem(new TableKeysAndAttributes(tableName).addHashAndRangePrimaryKeys(PRIMARY_KEY, PARTITION_PATH_COLUMN, alternateKeys.toArray(new String[alternateKeys.size()])));
      return outcome.getTableItems();
    }
    return null;
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  private Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> locationTagFunction(
      HoodieTableMetaClient metaClient) {
    return (Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>>) (partitionNum,
                                                                                       hoodieRecordIterator) -> {
      boolean updatePartitionPath = true;
      //RateLimiter limiter = RateLimiter.create(10 * 10, TimeUnit.SECONDS);
      synchronized (SparkHoodieDynamodbIndex.class) {
        if (dynamodbConnection == null) {
          dynamodbConnection = getConnection();
        }
      }
      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      //Table dtable = dynamodbConnection.getTable(tableName);
      List<HoodieRecord> currentRecord = new LinkedList<>();
      List<HoodieKey> statements = new ArrayList<>();
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord rec = hoodieRecordIterator.next();
        currentRecord.add(rec);
        statements.add(rec.getKey());
        if (hoodieRecordIterator.hasNext() && statements.size() < 10) {
          continue;
        }
        Map<String, List<Item>> results = doget(statements);
        statements.clear();
        for (Item result : results.get(tableName)) {
          HoodieRecord currentR = currentRecord.remove(0);
          String commitTs = result.getString(COMMIT_TS_COLUMN);
          String fieldId = result.getString(FILE_NAME_COLUMN);
          String partitionPath = result.getString(PARTITION_PATH_COLUMN);
          if (!checkIfValidCommit(metaClient, commitTs)) {
            taggedRecords.add(currentR);
            continue;
          }
          if (updatePartitionPath && !partitionPath.equals(currentR.getPartitionPath())) {
            HoodieRecord emptyRecord = new HoodieRecord(new HoodieKey(currentR.getRecordKey(), partitionPath),
                new EmptyHoodieRecordPayload());
            emptyRecord.unseal();
            emptyRecord.setCurrentLocation(new HoodieRecordLocation(commitTs, fieldId));
            emptyRecord.seal();
            currentR = new HoodieRecord(new HoodieKey(currentR.getRecordKey(), currentR.getPartitionPath()),
                currentR.getData());
            taggedRecords.add(emptyRecord);
          } else {
            currentR = new HoodieRecord(new HoodieKey(currentR.getRecordKey(), partitionPath),
                currentR.getData());
            currentR.unseal();
            currentR.setCurrentLocation(new HoodieRecordLocation(commitTs, fieldId));
            currentR.seal();
          }
          taggedRecords.add(currentR);
        }
      }
      return taggedRecords.iterator();
    };
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return false;
  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in Dynamodb already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  public Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> updateLocationFunction() {
    return (Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>>) (partition, statusIterator) -> {
      List<WriteStatus> writeStatusList = new ArrayList<>();
      synchronized (SparkHoodieDynamodbIndex.class) {
        if (dynamodbConnection == null) {
          dynamodbConnection = getConnection();
        }
      }
      final long StartTimeForPutsTask = DateTime.now().getMillis();
      LOG.info("Start Time for the task " + StartTimeForPutsTask);
      List<Item> huditablewriteItems = new ArrayList<>();
      while (statusIterator.hasNext()) {
        WriteStatus writeStatus = statusIterator.next();
        long numOfInserts = writeStatus.getStat().getNumInserts();
        for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
          if (!writeStatus.isErrored(rec.getKey())) {
            Option<HoodieRecordLocation> loc = rec.getNewLocation();
            if (loc.isPresent()) {
              if (rec.getCurrentLocation() != null) {
                continue;
              }
              huditablewriteItems.add(
                  new Item()
                      .withPrimaryKey(PRIMARY_KEY, rec.getRecordKey(), PARTITION_PATH_COLUMN, rec.getPartitionPath())
                      .withString(COMMIT_TS_COLUMN, loc.get().getInstantTime())
                      .withString(FILE_NAME_COLUMN, loc.get().getFileId())
              );
            }
          }
        }
        writeStatusList.add(writeStatus);
      }
      doCrud(huditablewriteItems);
      return writeStatusList.iterator();
    };
  }

  public void doCrud(List<Item> data) {
    if (data.isEmpty()) {
      return;
    }
    TableWriteItems hudiTable = new TableWriteItems(tableName);
    hudiTable.withItemsToPut(data);
    LOG.info("The Crud Operation is =");
    LOG.info(data);
    BatchWriteItemOutcome batchWriteItemOutcome = getConnection().batchWriteItem(hudiTable);
    LOG.info("The output is =");
    LOG.info(batchWriteItemOutcome);
  }

  public Map<String, Integer> mapFileWithInsertsToUniquePartition(JavaRDD<WriteStatus> writeStatusRDD) {
    final Map<String, Integer> fileIdPartitionMap = new HashMap<>();
    int partitionIndex = 0;
    // Map each fileId that has inserts to a unique partition Id. This will be used while
    // repartitioning RDD<WriteStatus>
    final List<String> fileIds = writeStatusRDD.filter(w -> w.getStat().getNumInserts() > 0)
        .map(w -> w.getFileId()).collect();
    for (final String fileId : fileIds) {
      fileIdPartitionMap.put(fileId, partitionIndex++);
    }
    return fileIdPartitionMap;
  }

  @Override
  public HoodieData<HoodieRecord<T>> tagLocation(HoodieData<HoodieRecord<T>> records, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    return HoodieJavaRDD.of(HoodieJavaRDD.getJavaRDD(records)
        .mapPartitionsWithIndex(locationTagFunction(hoodieTable.getMetaClient()), true));
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    JavaRDD<WriteStatus> writeStatusRDD = HoodieJavaRDD.getJavaRDD(writeStatuses);
    final Map<String, Integer> fileIdPartitionMap = mapFileWithInsertsToUniquePartition(writeStatusRDD);
    JavaRDD<WriteStatus> partitionedRDD = this.numWriteStatusWithInserts == 0 ? writeStatusRDD :
        writeStatusRDD.mapToPair(w -> new Tuple2<>(w.getFileId(), w))
            .partitionBy(new SparkHoodieHBaseIndex.WriteStatusPartitioner(fileIdPartitionMap,
                this.numWriteStatusWithInserts))
            .map(w -> w._2());
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    JavaRDD<WriteStatus> writeStatusJavaRDD = partitionedRDD.mapPartitionsWithIndex(updateLocationFunction(),
        true);
    // caching the index updated status RDD
    writeStatusJavaRDD = writeStatusJavaRDD.persist(SparkMemoryUtils.getWriteStatusStorageLevel(config.getProps()));
    // force trigger update location(hbase puts)
    writeStatusJavaRDD.count();
    return HoodieJavaRDD.of(writeStatusJavaRDD);
  }

  public void setDynamodbConnection(DynamoDB dynamodbConnection) {
    SparkHoodieDynamodbIndex.dynamodbConnection = dynamodbConnection;
  }
}