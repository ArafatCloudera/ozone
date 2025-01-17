/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * Class to iterate over the OM DB and populate the Recon container DB with
 * the container -&gt; Key reverse mapping.
 */
public class ContainerKeyMapperTask implements ReconOmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerKeyMapperTask.class);

  private ReconContainerMetadataManager reconContainerMetadataManager;
  private final long containerKeyFlushToDBMaxThreshold;

  @Inject
  public ContainerKeyMapperTask(ReconContainerMetadataManager
                                        reconContainerMetadataManager,
                                OzoneConfiguration configuration) {
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.containerKeyFlushToDBMaxThreshold = configuration.getLong(
        ReconServerConfigKeys.
            OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD,
        ReconServerConfigKeys.
            OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT
    );
  }

  /**
   * Read Key -&gt; ContainerId data from OM snapshot DB and write reverse map
   * (container, key) -&gt; count to Recon Container DB.
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    // Instead of a long for-loop, we go parallel.

    // Use concurrency-friendly maps so both threads can update them safely.
    // Alternatively, use per-thread maps and merge them later.
    ConcurrentHashMap<ContainerKeyPrefix, Integer> containerKeyMap =
        new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, Long> containerKeyCountMap =
        new ConcurrentHashMap<>();

    // Keep track of total keys processed, for logging.
    AtomicLong totalOmKeyCount = new AtomicLong(0);

    try {
      LOG.info("Starting a 'reprocess' run of ContainerKeyMapperTask.");
      Instant start = Instant.now();

      // 1) Initialize (wipe) the Recon container DB so we start fresh.
      reconContainerMetadataManager.reinitWithNewContainerDataFromOm(new HashMap<>());

      // 2) Create a thread pool with as many threads as bucket layouts, or
      //    pick a number that suits your CPU/hardware.
      ExecutorService executor = Executors.newFixedThreadPool(2);

      // 3) We have these two bucket layouts to process:
      List<BucketLayout> layouts = Arrays.asList(
          BucketLayout.LEGACY,
          BucketLayout.FILE_SYSTEM_OPTIMIZED
      );

      // Keep track of each thread’s result.
      List<Future<Boolean>> futures = new ArrayList<>();

      // 4) For each layout, submit a parallel scanning task.
      for (BucketLayout layout : layouts) {
        Future<Boolean> future = executor.submit(() -> {
          // Each thread scans one bucket layout.
          try {
            Table<String, OmKeyInfo> omKeyInfoTable =
                omMetadataManager.getKeyTable(layout);

            try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
                     keyIter = omKeyInfoTable.iterator()) {

              while (keyIter.hasNext()) {
                Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
                OmKeyInfo omKeyInfo = kv.getValue();

                // Transform & record in containerKeyMap / containerKeyCountMap.
                // Because these are concurrent maps, we can safely update them,
                // but consider performance impacts of concurrent writes.
                handleKeyReprocess(kv.getKey(), omKeyInfo,
                    containerKeyMap, containerKeyCountMap);

                // Increment total key count (thread-safe).
                totalOmKeyCount.incrementAndGet();

                // Optionally, check if we need to flush to DB due to threshold.
                // Must ensure only one thread flushes at a time!
                synchronized (ContainerKeyMapperTask.this) {
                  if (!checkAndCallFlushToDB(containerKeyMap)) {
                    LOG.error("Unable to flush containerKey info to DB (layout {}).", layout);
                    return false;
                  }
                }
              } // end while
            }
            return true;  // success
          } catch (Exception e) {
            LOG.error("Error while processing BucketLayout = {}", layout, e);
            return false;
          }
        });
        futures.add(future);
      }

      // 5) Wait for both tasks to complete.
      boolean allSuccessful = true;
      for (Future<Boolean> f : futures) {
        // get() blocks until thread finishes
        boolean threadResult = f.get();
        if (!threadResult) {
          allSuccessful = false;
        }
      }

      // Shut down threads. If you have other tasks to run in same pool, skip shutdown.
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.MINUTES);

      if (!allSuccessful) {
        LOG.error("One or more parallel tasks failed during reprocess().");
        return new ImmutablePair<>(getTaskName(), false);
      }

      // 6) Do a final flush of leftover data if needed:
      if (!flushAndCommitContainerKeyInfoToDB(containerKeyMap, containerKeyCountMap)) {
        LOG.error("Unable to flush leftover containerKey info after parallel scans.");
        return new ImmutablePair<>(getTaskName(), false);
      }

      // 7) Done! Log how long it took, plus how many keys we processed.
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("Completed 'reprocess' of ContainerKeyMapperTask in {} sec, processed {} keys total.",
          (double) duration / 1000.0, totalOmKeyCount.get());

      return new ImmutablePair<>(getTaskName(), true);

    } catch (InterruptedException ie) {
      LOG.error("reprocess() was interrupted.", ie);
      // Restore interrupt status
      Thread.currentThread().interrupt();
      return new ImmutablePair<>(getTaskName(), false);
    } catch (ExecutionException ee) {
      LOG.error("Error while waiting for parallel tasks in reprocess().", ee);
      return new ImmutablePair<>(getTaskName(), false);
    } catch (IOException ioEx) {
      LOG.error("Unable to re-init or flush container key data in Recon DB.", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
  }


  private boolean flushAndCommitContainerKeyInfoToDB(
      Map<ContainerKeyPrefix, Integer> containerKeyMap,
      Map<Long, Long> containerKeyCountMap) {
    try {
      // deleted container list is not needed since "reprocess" only has
      // put operations
      writeToTheDB(containerKeyMap, containerKeyCountMap,
          Collections.emptyList());
      containerKeyMap.clear();
      containerKeyCountMap.clear();
    } catch (IOException e) {
      LOG.error("Unable to write Container Key and " +
          "Container Key Count data in Recon DB.", e);
      return false;
    }
    return true;
  }

  private boolean checkAndCallFlushToDB(
      Map<ContainerKeyPrefix, Integer> containerKeyMap) {
    // if containerKeyMap more than entries, flush to DB and clear the map
    if (null != containerKeyMap && containerKeyMap.size() >=
          containerKeyFlushToDBMaxThreshold) {
      return flushAndCommitContainerKeyInfoToDB(containerKeyMap,
          Collections.emptyMap());
    }
    return true;
  }

  @Override
  public String getTaskName() {
    return "ContainerKeyMapperTask";
  }

  public Collection<String> getTaskTables() {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(KEY_TABLE);
    taskTables.add(FILE_TABLE);
    return taskTables;
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    int eventCount = 0;
    final Collection<String> taskTables = getTaskTables();

    // In-memory maps for fast look up and batch write
    // (HDDS-8580) containerKeyMap map is allowed to be used
    // in "process" without batching since the maximum number of keys
    // is bounded by delta limit configurations

    // (container, key) -> count
    Map<ContainerKeyPrefix, Integer> containerKeyMap = new HashMap<>();
    // containerId -> key count
    Map<Long, Long> containerKeyCountMap = new HashMap<>();
    // List of the deleted (container, key) pair's
    List<ContainerKeyPrefix> deletedKeyCountList = new ArrayList<>();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      // Filter event inside process method to avoid duping
      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo updatedKeyValue = omdbUpdateEvent.getValue();
      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList);
          break;

        case DELETE:
          handleDeleteOMKeyEvent(updatedKey, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList);
          break;

        case UPDATE:
          if (omdbUpdateEvent.getOldValue() != null) {
            handleDeleteOMKeyEvent(
                omdbUpdateEvent.getOldValue().getKeyName(), containerKeyMap,
                containerKeyCountMap, deletedKeyCountList);
          } else {
            LOG.warn("Update event does not have the old Key Info for {}.",
                updatedKey);
          }
          handlePutOMKeyEvent(updatedKey, updatedKeyValue, containerKeyMap,
              containerKeyCountMap, deletedKeyCountList);
          break;

        default: LOG.info("Skipping DB update event : {}",
            omdbUpdateEvent.getAction());
        }
        eventCount++;
      } catch (IOException e) {
        LOG.error("Unexpected exception while updating key data : {} ",
            updatedKey, e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    try {
      writeToTheDB(containerKeyMap, containerKeyCountMap, deletedKeyCountList);
    } catch (IOException e) {
      LOG.error("Unable to write Container Key Prefix data in Recon DB.", e);
      return new ImmutablePair<>(getTaskName(), false);
    }
    LOG.info("{} successfully processed {} OM DB update event(s).",
        getTaskName(), eventCount);
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void writeToTheDB(Map<ContainerKeyPrefix, Integer> containerKeyMap,
                            Map<Long, Long> containerKeyCountMap,
                            List<ContainerKeyPrefix> deletedContainerKeyList)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      containerKeyMap.keySet().forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager
              .batchStoreContainerKeyMapping(rdbBatchOperation, key,
                  containerKeyMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });


      containerKeyCountMap.keySet().forEach((Long key) -> {
        try {
          reconContainerMetadataManager
              .batchStoreContainerKeyCounts(rdbBatchOperation, key,
                  containerKeyCountMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });

      deletedContainerKeyList.forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager
              .batchDeleteContainerMapping(rdbBatchOperation, key);
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });

      reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    }
  }

  /**
   * Note to delete an OM Key and update the containerID -> no. of keys counts
   * (we are preparing for batch deletion in these data structures).
   *
   * @param key key String.
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        (in this batch)
   * @param containerKeyCountMap we keep the containerKey counts in this map
   * @param deletedContainerKeyList list of the deleted containerKeys
   * @throws IOException If Unable to write to container DB.
   */
  private void handleDeleteOMKeyEvent(String key,
                                      Map<ContainerKeyPrefix, Integer>
                                          containerKeyMap,
                                      Map<Long, Long> containerKeyCountMap,
                                      List<ContainerKeyPrefix>
                                          deletedContainerKeyList)
      throws IOException {

    Set<ContainerKeyPrefix> keysToBeDeleted = new HashSet<>();
    try (TableIterator<KeyPrefixContainer, ? extends
        Table.KeyValue<KeyPrefixContainer, Integer>> keyContainerIterator =
             reconContainerMetadataManager.getKeyContainerTableIterator()) {

      // Check if we have keys in this container in the DB
      keyContainerIterator.seek(KeyPrefixContainer.get(key));
      while (keyContainerIterator.hasNext()) {
        Table.KeyValue<KeyPrefixContainer, Integer> keyValue =
            keyContainerIterator.next();
        String keyPrefix = keyValue.getKey().getKeyPrefix();
        if (keyPrefix.equals(key)) {
          if (keyValue.getKey().getContainerId() != -1) {
            keysToBeDeleted.add(keyValue.getKey().toContainerKeyPrefix());
          }
        } else {
          break;
        }
      }
    }

    // Check if we have keys in this container in our containerKeyMap
    containerKeyMap.keySet()
        .forEach((ContainerKeyPrefix containerKeyPrefix) -> {
          String keyPrefix = containerKeyPrefix.getKeyPrefix();
          if (keyPrefix.equals(key)) {
            keysToBeDeleted.add(containerKeyPrefix);
          }
        });

    for (ContainerKeyPrefix containerKeyPrefix : keysToBeDeleted) {
      deletedContainerKeyList.add(containerKeyPrefix);
      // Remove the container-key prefix from the map if we previously added
      // it in this batch (and now we delete it)
      containerKeyMap.remove(containerKeyPrefix);

      // decrement count and update containerKeyCount.
      Long containerID = containerKeyPrefix.getContainerId();
      long keyCount;
      if (containerKeyCountMap.containsKey(containerID)) {
        keyCount = containerKeyCountMap.get(containerID);
      } else {
        keyCount = reconContainerMetadataManager
            .getKeyCountForContainer(containerID);
      }
      if (keyCount > 0) {
        containerKeyCountMap.put(containerID, --keyCount);
      }
    }
  }

  /**
   * Note to add an OM key and update containerID -> no. of keys count.
   *
   * @param key key String
   * @param omKeyInfo omKeyInfo value
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        (in this batch)
   * @param containerKeyCountMap we keep the containerKey counts in this map
   * @param deletedContainerKeyList list of the deleted containerKeys
   * @throws IOException if unable to write to recon DB.
   */
  private void handlePutOMKeyEvent(String key, OmKeyInfo omKeyInfo,
                                   Map<ContainerKeyPrefix, Integer>
                                       containerKeyMap,
                                   Map<Long, Long> containerKeyCountMap,
                                   List<ContainerKeyPrefix>
                                       deletedContainerKeyList)
      throws IOException {
    long containerCountToIncrement = 0;
    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo
        .getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup
          .getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(
            containerId, key, keyVersion);
        if (reconContainerMetadataManager.getCountForContainerKeyPrefix(
            containerKeyPrefix) == 0
            && !containerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix
          // mapping again.
          containerKeyMap.put(containerKeyPrefix, 1);
          // Remove the container-key prefix from the deleted list if we
          // previously deleted it in this batch (and now we add it again)
          deletedContainerKeyList.remove(containerKeyPrefix);


          // check if container already exists and
          // increment the count of containers if it does not exist
          if (!reconContainerMetadataManager.doesContainerExists(containerId)
              && !containerKeyCountMap.containsKey(containerId)) {
            containerCountToIncrement++;
          }

          // update the count of keys for the given containerID
          long keyCount;
          if (containerKeyCountMap.containsKey(containerId)) {
            keyCount = containerKeyCountMap.get(containerId);
          } else {
            keyCount = reconContainerMetadataManager
                .getKeyCountForContainer(containerId);
          }

          // increment the count and update containerKeyCount.
          // keyCount will be 0 if containerID is not found. So, there is no
          // need to initialize keyCount for the first time.
          containerKeyCountMap.put(containerId, ++keyCount);
        }
      }
    }

    if (containerCountToIncrement > 0) {
      reconContainerMetadataManager
          .incrementContainerCountBy(containerCountToIncrement);
    }
  }

  /**
   * Write an OM key to container DB and update containerID -> no. of keys
   * count to the Global Stats table.
   *
   * @param key key String
   * @param omKeyInfo omKeyInfo value
   * @param containerKeyMap we keep the added containerKeys in this map
   *                        to allow incremental batching to containerKeyTable
   * @param containerKeyCountMap we keep the containerKey counts in this map
   *                             to allow batching to containerKeyCountTable
   *                             after reprocessing is done
   * @throws IOException if unable to write to recon DB.
   */
  private void handleKeyReprocess(String key,
                                  OmKeyInfo omKeyInfo,
                                  Map<ContainerKeyPrefix, Integer>
                                      containerKeyMap,
                                  Map<Long, Long> containerKeyCountMap)
      throws IOException {
    long containerCountToIncrement = 0;
    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo
        .getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup
          .getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(
            containerId, key, keyVersion);
        if (reconContainerMetadataManager.getCountForContainerKeyPrefix(
            containerKeyPrefix) == 0
            && !containerKeyMap.containsKey(containerKeyPrefix)) {
          // Save on writes. No need to save same container-key prefix
          // mapping again.
          containerKeyMap.put(containerKeyPrefix, 1);

          // check if container already exists and
          // if it exists, update the count of keys for the given containerID
          // else, increment the count of containers and initialize keyCount
          long keyCount;
          if (containerKeyCountMap.containsKey(containerId)) {
            keyCount = containerKeyCountMap.get(containerId);
          } else {
            containerCountToIncrement++;
            keyCount = 0;
          }

          // increment the count and update containerKeyCount.
          containerKeyCountMap.put(containerId, ++keyCount);
        }
      }
    }

    if (containerCountToIncrement > 0) {
      reconContainerMetadataManager
          .incrementContainerCountBy(containerCountToIncrement);
    }
  }

}
