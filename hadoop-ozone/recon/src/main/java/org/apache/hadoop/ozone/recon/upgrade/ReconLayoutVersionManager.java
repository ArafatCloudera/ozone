/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.upgrade;

import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconSchemaVersionTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * ReconLayoutVersionManager is responsible for managing the layout version of the Recon service.
 * It determines the current Metadata Layout Version (MLV) and Software Layout Version (SLV) of the
 * Recon service, and finalizes the layout features that need to be upgraded.
 */
public class ReconLayoutVersionManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReconLayoutVersionManager.class);

  private final ReconSchemaVersionTableManager schemaVersionTableManager;
  private final ReconContext reconContext;

  // Metadata Layout Version (MLV) of the Recon Metadata on disk
  private int currentMLV;

  public ReconLayoutVersionManager(ReconSchemaVersionTableManager schemaVersionTableManager,
                                   ReconContext reconContext) {
    this.schemaVersionTableManager = schemaVersionTableManager;
    this.currentMLV = determineMLV();
    this.reconContext = reconContext;
    ReconLayoutFeature.registerUpgradeActions();  // Register actions via annotation
  }

  /**
   * Determines the current Metadata Layout Version (MLV) from the version table.
   * @return The current Metadata Layout Version (MLV).
   */
  private int determineMLV() {
    return schemaVersionTableManager.getCurrentSchemaVersion();
  }

  /**
   * Determines the Software Layout Version (SLV) based on the latest feature version.
   * @return The Software Layout Version (SLV).
   */
  private int determineSLV() {
    return Arrays.stream(ReconLayoutFeature.values())
        .mapToInt(ReconLayoutFeature::getVersion)
        .max()
        .orElse(0); // Default to 0 if no features are defined
  }

  /**
   * Finalizes the layout features that need to be upgraded, by executing the upgrade action for each
   * feature that is registered for finalization.
   */
  public void finalizeLayoutFeatures() {
    // Get features that need finalization, sorted by version
    List<ReconLayoutFeature> featuresToFinalize = getRegisteredFeatures();

    for (ReconLayoutFeature feature : featuresToFinalize) {
      try {
        // If the feature is INITIAL_VERSION, skip executing any action and just update the schema version
        if (feature == ReconLayoutFeature.INITIAL_VERSION) {
          updateSchemaVersion(0);
          LOG.info("INITIAL_VERSION feature processed by setting schema version to 0.");
          continue;
        }

        // Fetch only the FINALIZE action for the feature
        Optional<ReconUpgradeAction> action = feature.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE);
        if (action.isPresent()) {
          // Execute the upgrade action & update the schema version in the DB
          action.get().execute();
          updateSchemaVersion(feature.getVersion());
          LOG.info("Feature versioned {} finalized successfully.", feature.getVersion());
        }
      } catch (Exception e) {
        // Log the error to both logs and ReconContext
        LOG.error("Failed to finalize feature {}: {}", feature.getVersion(), e.getMessage());
        reconContext.updateErrors(ReconContext.ErrorCode.UPGRADE_FAILURE);
        reconContext.updateHealthStatus(new AtomicBoolean(false));
        break;
      }
    }
  }

  /**
   * Returns a list of ReconLayoutFeature objects that are registered for finalization.
   */
  protected List<ReconLayoutFeature> getRegisteredFeatures() {
    List<ReconLayoutFeature> allFeatures =
        Arrays.asList(ReconLayoutFeature.values());

    LOG.info("Current MLV: {}. SLV: {}. Checking features for registration...", currentMLV, determineSLV());

    List<ReconLayoutFeature> registeredFeatures = allFeatures.stream()
        .filter(feature -> feature.getVersion() > currentMLV)
        .sorted((a, b) -> Integer.compare(a.getVersion(), b.getVersion())) // Sort by version in ascending order
        .collect(Collectors.toList());

    return registeredFeatures;
  }

  /**
   * Updates the Software Layout Version (SLV) in the database after finalizing a feature.
   * @param newVersion The new Software Layout Version (SLV) to set.
   */
  private void updateSchemaVersion(int newVersion) {
    // Logic to update the MLV in the database
    this.currentMLV = newVersion;
    // Code to update the schema version in the database goes here.
    schemaVersionTableManager.updateSchemaVersion(newVersion);
    LOG.info("MLV updated to: " + newVersion);
  }

  public int getCurrentMLV() {
    return currentMLV;
  }

  public int getCurrentSLV() {
    return determineSLV();
  }

}
