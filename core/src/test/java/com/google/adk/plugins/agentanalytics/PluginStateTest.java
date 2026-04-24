/*
 * Copyright 2026 Google LLC
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

package com.google.adk.plugins.agentanalytics;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public final class PluginStateTest {
  private BigQueryLoggerConfig config;
  private TestPluginState pluginState;
  private Handler mockHandler;
  private Logger pluginLogger;
  private Level originalLevel;

  private static class TestPluginState extends PluginState {
    TestPluginState(BigQueryLoggerConfig config) throws IOException {
      super(config);
    }

    @Override
    protected BigQueryWriteClient createWriteClient(BigQueryLoggerConfig config) {
      return mock(BigQueryWriteClient.class);
    }
  }

  @Before
  public void setUp() throws IOException {
    config =
        BigQueryLoggerConfig.builder()
            .projectId("test-project")
            .datasetId("test-dataset")
            .tableName("test-table")
            .gcsBucketName("")
            .build();
    pluginState = new TestPluginState(config);

    pluginLogger = Logger.getLogger(PluginState.class.getName());
    mockHandler = mock(Handler.class);
    originalLevel = pluginLogger.getLevel();
    pluginLogger.setLevel(Level.INFO);
    pluginLogger.addHandler(mockHandler);
  }

  @After
  public void tearDown() {
    pluginLogger.removeHandler(mockHandler);
    pluginLogger.setLevel(originalLevel);
  }

  @Test
  public void getGcsOffloader_emptyBucketName_returnsNull() {
    assertNull(pluginState.getGcsOffloader(config));
  }

  @Test
  public void addPendingTask_logsWaitingInWaitForPendingTasks() {
    String invocationId = "testInvocation";
    CompletableFuture<Void> task = new CompletableFuture<>();
    pluginState.addPendingTask(invocationId, task);

    // Complete the task asynchronously so waitForPendingTasks can return.
    var unused =
        CompletableFuture.runAsync(
            () -> {
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              task.complete(null);
            });

    pluginState.waitForPendingTasks(invocationId);

    ArgumentCaptor<LogRecord> captor = ArgumentCaptor.forClass(LogRecord.class);
    verify(mockHandler, atLeastOnce()).publish(captor.capture());

    boolean found =
        captor.getAllValues().stream()
            .anyMatch(record -> record.getMessage().contains("Waiting for 1 pending tasks"));
    assertTrue("Expected log message 'Waiting for 1 pending tasks' not found", found);
  }

  @Test
  public void waitForPendingTasks_noTasks_doesNotLogWaiting() {
    String invocationId = "testInvocation";

    pluginState.waitForPendingTasks(invocationId);

    // Verify that "Waiting for X pending tasks..." is NOT logged.
    // We check if any log record contains "Waiting for " using ArgumentCaptor if needed,
    // but a simple verify on publish with message check is better.
    verify(mockHandler, never()).publish(any(LogRecord.class));
  }

  @Test
  public void waitForPendingTasks_timeout_logsWarning() throws IOException {
    config = config.toBuilder().shutdownTimeout(Duration.ofMillis(100)).build();
    pluginState = new TestPluginState(config);

    String invocationId = "testInvocation";
    CompletableFuture<Void> task = new CompletableFuture<>(); // Never completes
    pluginState.addPendingTask(invocationId, task);

    pluginState.waitForPendingTasks(invocationId);

    ArgumentCaptor<LogRecord> captor = ArgumentCaptor.forClass(LogRecord.class);
    verify(mockHandler, atLeastOnce()).publish(captor.capture());

    boolean found =
        captor.getAllValues().stream()
            .anyMatch(
                record ->
                    record.getLevel().equals(Level.WARNING)
                        && record.getMessage().contains("Timeout while waiting for pending tasks"));
    assertTrue("Expected log message 'Timeout while waiting for pending tasks' not found", found);
  }

  @Test
  public void waitForPendingTasks_executionException_logsWarning() throws InterruptedException {
    String invocationId = "testInvocation";
    CompletableFuture<Void> task = new CompletableFuture<>();
    pluginState.addPendingTask(invocationId, task);

    // Complete the task asynchronously to ensure it is present when waitForPendingTasks starts.
    var unused =
        CompletableFuture.runAsync(
            () -> {
              try {
                Thread.sleep(20);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              task.completeExceptionally(new RuntimeException("test exception"));
            });

    pluginState.waitForPendingTasks(invocationId);

    ArgumentCaptor<LogRecord> captor = ArgumentCaptor.forClass(LogRecord.class);
    verify(mockHandler, atLeastOnce()).publish(captor.capture());

    boolean found =
        captor.getAllValues().stream()
            .anyMatch(
                record ->
                    record.getLevel().equals(Level.WARNING)
                        && record.getMessage().contains("One or more pending tasks failed"));
    assertTrue("Expected log message 'One or more pending tasks failed' not found", found);
  }

  @Test
  public void waitForPendingTasks_interrupted_logsWarning() throws InterruptedException {
    String invocationId = "testInvocation";
    CompletableFuture<Void> task = new CompletableFuture<>();
    pluginState.addPendingTask(invocationId, task);

    Thread testThread =
        new Thread(
            () -> {
              pluginLogger.addHandler(mockHandler);
              pluginState.waitForPendingTasks(invocationId);
            });
    testThread.start();
    Thread.sleep(50);
    testThread.interrupt();
    testThread.join(1000);

    ArgumentCaptor<LogRecord> captor = ArgumentCaptor.forClass(LogRecord.class);
    verify(mockHandler, atLeastOnce()).publish(captor.capture());

    boolean found =
        captor.getAllValues().stream()
            .anyMatch(
                record ->
                    record.getLevel().equals(Level.WARNING)
                        && record
                            .getMessage()
                            .contains("Interrupted while waiting for pending tasks"));
    assertTrue(
        "Expected log message 'Interrupted while waiting for pending tasks' not found", found);
  }
}
