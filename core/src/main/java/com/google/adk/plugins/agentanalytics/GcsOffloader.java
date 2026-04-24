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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auth.Credentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/** Offloads content to GCS. */
class GcsOffloader {
  private final Storage storage;
  private final String bucketName;
  private final Executor executor;

  GcsOffloader(
      String projectId,
      String bucketName,
      Executor executor,
      @Nullable Credentials credentials,
      @Nullable Storage storageOverride) {
    if (storageOverride != null) {
      this.storage = storageOverride;
    } else {
      StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(projectId);
      if (credentials != null) {
        builder.setCredentials(credentials);
      }
      this.storage = builder.build().getService();
    }
    this.bucketName = bucketName;
    this.executor = executor;
  }

  /** Async wrapper around blocking GCS upload for binary data. */
  CompletableFuture<String> uploadContent(byte[] data, String contentType, String path) {
    return CompletableFuture.supplyAsync(
        () -> {
          BlobId blobId = BlobId.of(bucketName, path);
          BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();
          storage.create(blobInfo, data);
          return String.format("gs://%s/%s", bucketName, path);
        },
        executor);
  }

  /** Async wrapper around blocking GCS upload for text data. */
  CompletableFuture<String> uploadContent(String data, String contentType, String path) {
    return uploadContent(data.getBytes(UTF_8), contentType, path);
  }

  String getBucketName() {
    return bucketName;
  }

  void close() throws Exception {
    if (storage != null) {
      storage.close();
    }
  }
}
