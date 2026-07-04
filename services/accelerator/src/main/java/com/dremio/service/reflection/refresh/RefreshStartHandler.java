/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.reflection.refresh;

import static com.dremio.exec.planner.physical.PlannerSettings.MANUAL_REFLECTION_MODE;
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;
import static com.dremio.service.reflection.ReflectionUtils.getId;

import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.ReflectionDDLUtils;
import com.dremio.service.reflection.ReflectionManager;
import com.dremio.service.reflection.ReflectionManager.WakeUpCallback;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.WakeUpManagerWhenJobDone;
import com.dremio.service.reflection.materialization.AccelerationStoragePlugin;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.UUID;
import org.apache.iceberg.Table;

/** called when a materialization job is started */
public class RefreshStartHandler {

  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RefreshStartHandler.class);

  private final CatalogService catalogService;
  private final JobsService jobsService;
  private final ReflectionGoalsStore reflectionGoalStore;
  private final MaterializationStore materializationStore;
  private final WakeUpCallback wakeUpCallback;
  private final OptionManager optionManager;

  public RefreshStartHandler(
      CatalogService catalogService,
      JobsService jobsService,
      ReflectionGoalsStore reflectionGoalStore,
      MaterializationStore materializationStore,
      WakeUpCallback wakeUpCallback,
      OptionManager optionManager) {
    this.catalogService = Preconditions.checkNotNull(catalogService, "Catalog service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.reflectionGoalStore =
        Preconditions.checkNotNull(reflectionGoalStore, "goal store required");
    this.materializationStore =
        Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.wakeUpCallback = Preconditions.checkNotNull(wakeUpCallback, "wakeup callback required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "option manager required");
  }

  @WithSpan
  public JobId submitRefreshJob(
      JobsService jobsService,
      DatasetConfig datasetConfig,
      ReflectionEntry entry,
      MaterializationId materializationId,
      String sql,
      QueryType queryType,
      JobStatusListener jobStatusListener,
      OptionManager optionManager) {

    return ReflectionUtils.submitRefreshJob(
        jobsService,
        datasetConfig,
        entry,
        materializationId,
        sql,
        queryType,
        jobStatusListener,
        optionManager,
        getReflectionMode(entry.getId()));
  }

  @WithSpan
  public JobId startJob(ReflectionEntry entry, long jobSubmissionTime) {
    final ReflectionId reflectionId = entry.getId();

    final MaterializationId id = new MaterializationId(UUID.randomUUID().toString());
    final Materialization materialization =
        new Materialization()
            .setId(id)
            .setInitRefreshSubmit(jobSubmissionTime)
            .setState(MaterializationState.RUNNING)
            .setLastRefreshFromPds(0L)
            .setReflectionGoalVersion(entry.getGoalVersion())
            .setReflectionGoalHash(entry.getReflectionGoalHash())
            .setReflectionId(reflectionId);
    setIcebergReflectionAttributes(entry, materialization);
    // this is getting convoluted, but we need to make sure we save the materialization before we
    // run the CTAS
    // as the MaterializedView will need it to extract the logicalPlan
    materializationStore.save(materialization);

    final DatasetConfig datasetConfig = ReflectionUtils.getAnchorDataset(catalogService, entry);
    final String sql =
        String.format(
            "REFRESH REFLECTION '%s' AS '%s'\n/**\n%s\n*/",
            reflectionId.getId(),
            materialization.getId().getId(),
            generateReflectionDDL(entry.getId(), datasetConfig));
    final JobId jobId =
        submitRefreshJob(
            jobsService,
            datasetConfig,
            entry,
            materialization.getId(),
            sql,
            QueryType.ACCELERATOR_CREATE,
            new WakeUpManagerWhenJobDone(wakeUpCallback, "materialization job done"),
            optionManager);

    logger.info(
        "Submitted REFRESH REFLECTION job {} for {}",
        jobId.getId(),
        ReflectionUtils.getId(entry, materialization));

    materialization.setInitRefreshJobId(jobId.getId());

    materializationStore.save(materialization);
    ReflectionManager.setSpanAttributes(entry, materialization);

    return jobId;
  }

  private void setIcebergReflectionAttributes(
      ReflectionEntry entry, Materialization materialization) {
    final ReflectionId reflectionId = entry.getId();
    FluentIterable<Refresh> refreshes =
        materializationStore.getRefreshesByReflectionId(reflectionId);
    if (refreshes.isEmpty()) {
      // no previous refresh exists. nothing to set.
      return;
    }

    Refresh latestRefresh = refreshes.get(refreshes.size() - 1);
    // current refresh is using iceberg.
    if (latestRefresh.getIsIcebergRefresh() == null || !latestRefresh.getIsIcebergRefresh()) {
      // last refresh did not use Iceberg, hence forcing full refresh
      materialization.setForceFullRefresh(true);
      logger.debug(
          "Force full refresh materialization {} since last refresh is non-iceberg",
          getId(materialization));
    }
    if (latestRefresh.getIsIcebergRefresh() != null && latestRefresh.getIsIcebergRefresh()) {
      // current refresh is iceberg, and last refresh was also iceberg
      // set base path so that incremental refresh can insert into Iceberg table at base path
      materialization.setBasePath(latestRefresh.getBasePath());

      // only set previous Iceberg snapshot for incremental refresh
      if (entry.getRefreshMethod() == RefreshMethod.INCREMENTAL) {
        final AccelerationStoragePlugin accelerationPlugin =
            catalogService.getSource(ACCELERATOR_STORAGEPLUGIN_NAME);
        try {
          final Table table =
              ReflectionUtils.getIcebergTable(
                  reflectionId, latestRefresh.getBasePath(), accelerationPlugin);
          materialization.setPreviousIcebergSnapshot(table.currentSnapshot().snapshotId());
        } catch (IllegalStateException e) {
          // The physical path somehow was deleted, so we need to force a full refresh so we don't
          // get stuck.
          logger.warn(
              "Failed to get iceberg table for reflection {} at path {}",
              reflectionId,
              latestRefresh.getBasePath(),
              e);
          materialization
              .setPreviousIcebergSnapshot(null)
              .setBasePath(null)
              .setForceFullRefresh(true);
        }
      }
    }
  }

  protected String getReflectionMode(ReflectionId reflectionId) {
    return MANUAL_REFLECTION_MODE;
  }

  protected CatalogService getCatalogService() {
    return catalogService;
  }

  protected MaterializationStore getMaterializationStore() {
    return materializationStore;
  }

  protected ReflectionGoalsStore getReflectionGoalsStore() {
    return reflectionGoalStore;
  }

  private String generateReflectionDDL(ReflectionId reflectionId, DatasetConfig datasetConfig) {
    try {
      ReflectionGoal reflectionGoal = reflectionGoalStore.get(reflectionId);
      return ReflectionDDLUtils.generateDDLfromGoal(
          reflectionGoal, ReflectionDDLUtils.generateDatasetPathSQL(datasetConfig));
    } catch (Exception e) {
      logger.error("Failed to generate reflection DDL for reflection {}", reflectionId, e);
      return "ERROR GENERATING ALTER DATASET DDL";
    }
  }
}
