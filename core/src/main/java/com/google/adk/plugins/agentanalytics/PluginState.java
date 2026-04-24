package com.google.adk.plugins.agentanalytics;

import static com.google.adk.plugins.agentanalytics.BigQueryUtils.getVersionHeaderValue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jspecify.annotations.Nullable;
import org.threeten.bp.Duration;

/** Manages state for the BigQueryAgentAnalyticsPlugin. */
class PluginState {
  private static final Logger logger = Logger.getLogger(PluginState.class.getName());
  private final BigQueryLoggerConfig config;
  private final ScheduledExecutorService executor;
  private final ExecutorService offloadExecutor;
  private final BigQueryWriteClient writeClient;
  private static final AtomicLong threadCounter = new AtomicLong(0);
  // Map of invocation ID to BatchProcessor.
  private final ConcurrentHashMap<String, BatchProcessor> batchProcessors =
      new ConcurrentHashMap<>();
  // Map of invocation ID to TraceManager.
  private final ConcurrentHashMap<String, TraceManager> traceManagers = new ConcurrentHashMap<>();
  // Cache of invocation ID to Boolean indicating invocation ID has been processed.
  private final Cache<String, Boolean> processedInvocations;
  private final GcsOffloader offloader;
  private final Parser parser;
  private final ConcurrentHashMap<String, Set<CompletableFuture<Void>>> pendingTasks =
      new ConcurrentHashMap<>();

  PluginState(BigQueryLoggerConfig config) throws IOException {
    this.config = config;
    this.executor =
        Executors.newScheduledThreadPool(
            2, r -> new Thread(r, "bq-analytics-plugin-" + threadCounter.getAndIncrement()));
    this.offloadExecutor =
        Executors.newCachedThreadPool(
            r -> new Thread(r, "bq-analytics-plugin-offload-" + threadCounter.getAndIncrement()));
    // One write client per plugin instance, shared by all invocations.
    this.writeClient = createWriteClient(config);
    this.processedInvocations =
        CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(java.time.Duration.ofMinutes(10))
            .build();
    this.offloader = getGcsOffloader(config);
    this.parser =
        new Parser(
            offloader,
            config.maxContentLength(),
            config.connectionId().orElse(null),
            config.logMultiModalContent());
  }

  ScheduledExecutorService getExecutor() {
    return executor;
  }

  boolean isProcessed(String invocationId) {
    boolean isProcessed = processedInvocations.getIfPresent(invocationId) != null;
    if (isProcessed) {
      logger.info("Invocation ID: " + invocationId + "  already processed");
    }
    return isProcessed;
  }

  void markProcessed(String invocationId) {
    processedInvocations.put(invocationId, true);
  }

  protected BigQueryWriteClient createWriteClient(BigQueryLoggerConfig config) throws IOException {
    BigQueryWriteSettings.Builder settingsBuilder =
        BigQueryWriteSettings.newBuilder()
            .setHeaderProvider(
                FixedHeaderProvider.create(ImmutableMap.of("user-agent", getVersionHeaderValue())));
    if (config.credentials() != null) {
      settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(config.credentials()));
    }
    return BigQueryWriteClient.create(settingsBuilder.build());
  }

  protected StreamWriter createWriter() {
    BigQueryLoggerConfig.RetryConfig retryConfig = config.retryConfig();
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setMaxAttempts(retryConfig.maxRetries())
            .setInitialRetryDelay(Duration.ofMillis(retryConfig.initialDelay().toMillis()))
            .setRetryDelayMultiplier(retryConfig.multiplier())
            .setMaxRetryDelay(Duration.ofMillis(retryConfig.maxDelay().toMillis()))
            .build();

    String streamName = getStreamName(config);
    try {
      return StreamWriter.newBuilder(streamName, writeClient)
          .setTraceId(BigQueryUtils.getVersionHeaderValue() + ":" + UUID.randomUUID())
          .setRetrySettings(retrySettings)
          .setWriterSchema(BigQuerySchema.getArrowSchema())
          .build();
    } catch (Exception e) {
      throw new VerifyException("Failed to create StreamWriter for " + streamName, e);
    }
  }

  @VisibleForTesting
  String getStreamName(BigQueryLoggerConfig config) {
    return String.format(
        "projects/%s/datasets/%s/tables/%s/streams/_default",
        config.projectId(), config.datasetId(), config.tableName());
  }

  @VisibleForTesting
  TraceManager getTraceManager(String invocationId) {
    return traceManagers.computeIfAbsent(invocationId, id -> new TraceManager());
  }

  @VisibleForTesting
  BatchProcessor getBatchProcessor(String invocationId) {
    return batchProcessors.computeIfAbsent(
        invocationId,
        id -> {
          BatchProcessor p =
              new BatchProcessor(
                  createWriter(),
                  config.batchSize(),
                  config.batchFlushInterval(),
                  config.queueMaxSize(),
                  executor);
          p.start();
          return p;
        });
  }

  protected @Nullable GcsOffloader getGcsOffloader(BigQueryLoggerConfig config) {
    if (config.gcsBucketName().isEmpty()) {
      return null;
    }
    return new GcsOffloader(
        config.projectId(), config.gcsBucketName(), offloadExecutor, config.credentials(), null);
  }

  Parser getParser() {
    return parser;
  }

  @VisibleForTesting
  Collection<TraceManager> getTraceManagers() {
    return traceManagers.values();
  }

  @VisibleForTesting
  Collection<BatchProcessor> getBatchProcessors() {
    return batchProcessors.values();
  }

  @VisibleForTesting
  TraceManager removeTraceManager(String invocationId) {
    return traceManagers.remove(invocationId);
  }

  @VisibleForTesting
  protected BatchProcessor removeProcessor(String invocationId) {
    return batchProcessors.remove(invocationId);
  }

  void clearTraceManagers() {
    traceManagers.clear();
  }

  void clearBatchProcessors() {
    batchProcessors.clear();
  }

  private Set<CompletableFuture<Void>> getPendingTasksForInvocation(String invocationId) {
    return pendingTasks.computeIfAbsent(invocationId, k -> ConcurrentHashMap.newKeySet());
  }

  void addPendingTask(String invocationId, CompletableFuture<Void> task) {
    Set<CompletableFuture<Void>> tasks = getPendingTasksForInvocation(invocationId);
    tasks.add(task);
    var unused = task.whenComplete((res, err) -> tasks.remove(task));
  }

  void waitForPendingTasks(String invocationId) {
    Set<CompletableFuture<Void>> invocationTasks = pendingTasks.get(invocationId);
    if (invocationTasks == null || invocationTasks.isEmpty()) {
      pendingTasks.remove(invocationId);
      return;
    }
    ImmutableList<CompletableFuture<Void>> tasks = ImmutableList.copyOf(invocationTasks);
    logger.info(
        "Waiting for " + tasks.size() + " pending tasks for invocation ID: " + invocationId);
    try {
      CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0]))
          .get(config.shutdownTimeout().toMillis(), MILLISECONDS);
    } catch (TimeoutException e) {
      logger.log(
          Level.WARNING,
          "Timeout while waiting for pending tasks for invocation ID: " + invocationId,
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.log(
          Level.WARNING,
          "Interrupted while waiting for pending tasks for invocation ID: " + invocationId,
          e);
    } catch (ExecutionException e) {
      logger.log(
          Level.WARNING, "One or more pending tasks failed for invocation ID: " + invocationId, e);
    }
    logger.info("Finished waiting for pending tasks for invocation ID: " + invocationId);
    pendingTasks.remove(invocationId);
  }

  void close() {
    for (BatchProcessor processor : getBatchProcessors()) {
      processor.close();
    }
    for (TraceManager traceManager : getTraceManagers()) {
      traceManager.clearStack();
    }
    clearBatchProcessors();
    clearTraceManagers();

    if (writeClient != null) {
      writeClient.close();
    }
    try {
      executor.shutdown();
      offloadExecutor.shutdown();
      if (!executor.awaitTermination(config.shutdownTimeout().toMillis(), MILLISECONDS)) {
        executor.shutdownNow();
      }
      if (!offloadExecutor.awaitTermination(config.shutdownTimeout().toMillis(), MILLISECONDS)) {
        offloadExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      offloadExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    try {
      if (offloader != null) {
        offloader.close();
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Failed to close GCS offloader", e);
    }
  }
}
