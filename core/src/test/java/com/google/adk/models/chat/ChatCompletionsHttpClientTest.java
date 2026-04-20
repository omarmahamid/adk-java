/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.adk.models.chat;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.JsonBaseModel;
import com.google.adk.models.LlmRequest;
import com.google.adk.models.LlmResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.genai.types.Content;
import com.google.genai.types.FinishReason;
import com.google.genai.types.HttpOptions;
import com.google.genai.types.Part;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class ChatCompletionsHttpClientTest {
  private static final ObjectMapper objectMapper = JsonBaseModel.getMapper();
  private static final MediaType JSON = MediaType.get("application/json");

  /**
   * Bounded wait for {@link TestSubscriber#await(long, TimeUnit)} so a buggy callback wiring cannot
   * hang the test JVM. The mock callbacks fire synchronously in the same thread, so this value is
   * intentionally short -- on a successful run the await returns in microseconds, and on a hung run
   * we fail fast instead of stalling the test suite.
   */
  private static final long AWAIT_TIMEOUT_MILLIS = 500L;

  @Rule public final MockitoRule mocks = MockitoJUnit.rule();

  @Mock private OkHttpClient mockHttpClient;
  @Mock private Call mockCall;

  private ChatCompletionsHttpClient client;

  @Before
  public void setUp() throws Exception {
    client = new ChatCompletionsHttpClient("https://example.com/", ImmutableMap.of(), null);
    swapInMockHttpClient(client);
  }

  /**
   * Reflectively replaces the production {@link OkHttpClient} on a {@link
   * ChatCompletionsHttpClient} with the test's mock so callbacks can be captured. Used by both
   * setUp and tests that construct their own client (e.g. timeout tests, header tests).
   */
  private void swapInMockHttpClient(ChatCompletionsHttpClient target) throws Exception {
    when(mockHttpClient.newCall(any())).thenReturn(mockCall);
    Field clientField = ChatCompletionsHttpClient.class.getDeclaredField("client");
    clientField.setAccessible(true);
    clientField.set(target, mockHttpClient);
  }

  private Response createMockResponse(String body, MediaType mediaType) {
    return createMockResponse(body, mediaType, 200, "OK");
  }

  private Response createMockResponse(String body, MediaType mediaType, int code, String message) {
    Response.Builder builder =
        new Response.Builder()
            .request(new Request.Builder().url("https://example.com/chat/completions").build())
            .protocol(Protocol.HTTP_1_1)
            .code(code)
            .message(message);
    // OkHttp's Response.Builder rejects a null body via its Kotlin @NotNull contract; omit
    // the body() call entirely to model an empty/null response body.
    if (body != null) {
      builder.body(ResponseBody.create(body, mediaType));
    }
    return builder.build();
  }

  /** Returns a minimal {@link LlmRequest} suitable for tests that don't care about the payload. */
  private static LlmRequest minimalRequest() {
    return LlmRequest.builder()
        .model("gpt-4")
        .contents(ImmutableList.of(Content.builder().parts(Part.fromText("hello")).build()))
        .build();
  }

  @Test
  public void generateContent_nonStreaming_sendsCorrectPayload() throws Exception {
    String responseBody =
        """
        {
          "choices": [
            {
              "message": {
                "role": "assistant",
                "content": "Hi"
              },
              "finish_reason": "stop"
            }
          ]
        }
        """;

    Response mockResponse = createMockResponse(responseBody, JSON);

    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        client.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onResponse(mockCall, mockResponse);
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    LlmResponse response = testSubscriber.values().get(0);

    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(mockHttpClient).newCall(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();
    assertThat(capturedRequest.url().encodedPath()).isEqualTo("/chat/completions");

    Buffer buffer = new Buffer();
    capturedRequest.body().writeTo(buffer);
    JsonNode requestBodyJson = objectMapper.readTree(buffer.readUtf8());
    assertThat(requestBodyJson.get("model").asText()).isEqualTo("gpt-4");
    assertThat(requestBodyJson.get("messages").get(0).get("role").asText()).isEqualTo("user");
    assertThat(requestBodyJson.get("messages").get(0).get("content").asText()).isEqualTo("hello");

    LlmResponse expectedResponse =
        LlmResponse.builder()
            .content(
                Content.builder()
                    .role("model")
                    .parts(ImmutableList.of(Part.fromText("Hi")))
                    .build())
            .finishReason(new FinishReason(FinishReason.Known.STOP.toString()))
            .customMetadata(ImmutableList.of())
            .build();

    assertThat(response).isEqualTo(expectedResponse);
  }

  @Test
  public void generateContent_nonStreaming_propagateFailure() throws Exception {
    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        client.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onFailure(mockCall, new IOException("Network Error"));
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    testSubscriber.assertError(IOException.class);
  }

  // -- New tests for fixes #5, #6, #9, and additional coverage. --------------------------

  /**
   * Verifies that an HTTP error status (e.g. 500) propagates as a stream error and that the error
   * message includes the response body so callers can debug. Covers the {@code
   * !response.isSuccessful()} branch of the non-streaming path. The streaming counterpart lives in
   * the streaming follow-up CL.
   */
  @Test
  public void generateContent_nonStreaming_propagatesHttpErrorStatus() throws Exception {
    Response mockResponse =
        createMockResponse("{\"error\":\"server exploded\"}", JSON, 500, "Internal Server Error");

    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        client.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onResponse(mockCall, mockResponse);
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    testSubscriber.assertError(
        e ->
            e instanceof IOException
                && e.getMessage().contains("Unexpected code")
                && e.getMessage().contains("server exploded"));
  }

  /**
   * Verifies that an empty response body propagates as a stream error rather than silently emitting
   * an empty value. The exact exception class depends on OkHttp's behavior:
   *
   * <ul>
   *   <li>If OkHttp produces a {@code null} body, our code surfaces an {@link IOException} with the
   *       message {@code "Empty response body"}.
   *   <li>If OkHttp produces an empty (non-null) body, Jackson surfaces a {@link
   *       com.fasterxml.jackson.databind.exc.MismatchedInputException} ("No content to map").
   * </ul>
   *
   * Both outcomes satisfy the contract: empty body must NOT silently produce a successful empty
   * {@link LlmResponse}.
   */
  @Test
  public void generateContent_nonStreaming_propagatesEmptyBody() throws Exception {
    Response mockResponse = createMockResponse(null, JSON);

    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        client.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onResponse(mockCall, mockResponse);
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    testSubscriber.assertNoValues();
    testSubscriber.assertError(Throwable.class);
  }

  /**
   * Verifies that caller-supplied headers reach the wire on the captured {@link Request}. This is
   * the most common production failure mode (missing or wrong Authorization header), so it gets its
   * own test rather than being implicit in other tests.
   */
  @Test
  public void generateContent_sendsCustomHeaders() throws Exception {
    ChatCompletionsHttpClient clientWithHeaders =
        new ChatCompletionsHttpClient(
            "https://example.com/",
            ImmutableMap.of("Authorization", "Bearer test-token", "X-Custom", "value"),
            null);
    swapInMockHttpClient(clientWithHeaders);

    String responseBody =
        """
        {"choices":[{"message":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}
        """;
    Response mockResponse = createMockResponse(responseBody, JSON);

    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        clientWithHeaders.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onResponse(mockCall, mockResponse);
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(mockHttpClient).newCall(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();
    assertThat(capturedRequest.header("Authorization")).isEqualTo("Bearer test-token");
    assertThat(capturedRequest.header("X-Custom")).isEqualTo("value");
    // Verifies fix #5: Content-Type is forced to application/json regardless of caller input.
    assertThat(capturedRequest.header("Content-Type")).contains("application/json");
  }

  /**
   * Verifies fix #5: even when a caller passes a conflicting {@code Content-Type} header, the
   * client overrides it with {@code application/json} so the upstream API does not reject the
   * request as a malformed payload.
   */
  @Test
  public void generateContent_overridesCallerContentType() throws Exception {
    ChatCompletionsHttpClient clientWithBadHeader =
        new ChatCompletionsHttpClient(
            "https://example.com/", ImmutableMap.of("Content-Type", "text/plain"), null);
    swapInMockHttpClient(clientWithBadHeader);

    String responseBody =
        """
        {"choices":[{"message":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}
        """;
    Response mockResponse = createMockResponse(responseBody, JSON);

    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        clientWithBadHeader.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onResponse(mockCall, mockResponse);
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(mockHttpClient).newCall(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();
    // Should be exactly one Content-Type header, not two.
    assertThat(capturedRequest.headers("Content-Type")).hasSize(1);
    assertThat(capturedRequest.header("Content-Type")).contains("application/json");
  }

  /**
   * Verifies that a {@code baseUrl} without a trailing slash still produces the correct {@code
   * /chat/completions} path. Covers the false branch of the trailing-slash check in {@link
   * ChatCompletionsHttpClient#generateContent}.
   */
  @Test
  public void generateContent_handlesBaseUrlWithoutTrailingSlash() throws Exception {
    ChatCompletionsHttpClient clientNoSlash =
        new ChatCompletionsHttpClient("https://example.com", ImmutableMap.of(), null);
    swapInMockHttpClient(clientNoSlash);

    String responseBody =
        """
        {"choices":[{"message":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}
        """;
    Response mockResponse = createMockResponse(responseBody, JSON);

    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        clientNoSlash.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onResponse(mockCall, mockResponse);
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(mockHttpClient).newCall(requestCaptor.capture());
    assertThat(requestCaptor.getValue().url().encodedPath()).isEqualTo("/chat/completions");
  }

  /**
   * Verifies fix #9: passing {@code null} for the headers parameter is treated as no extra headers,
   * not as an NPE.
   */
  @Test
  public void constructor_nullHeaders_isTreatedAsEmpty() throws Exception {
    ChatCompletionsHttpClient clientWithNullHeaders =
        new ChatCompletionsHttpClient("https://example.com/", null, null);
    swapInMockHttpClient(clientWithNullHeaders);

    String responseBody =
        """
        {"choices":[{"message":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}
        """;
    Response mockResponse = createMockResponse(responseBody, JSON);

    ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(mockCall).enqueue(callbackCaptor.capture());

    TestSubscriber<LlmResponse> testSubscriber =
        clientWithNullHeaders.generateContent(minimalRequest(), false).test();

    callbackCaptor.getValue().onResponse(mockCall, mockResponse);
    testSubscriber.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    testSubscriber.assertNoErrors();
    testSubscriber.assertValueCount(1);
  }

  // -- Fix #6: tri-state timeout policy. --------------------------------------------------

  /**
   * Verifies fix #6 case (a): when {@code httpOptions} is {@code null}, the client applies the
   * 5-minute default call timeout to prevent indefinite hangs in callers that did not explicitly
   * configure a timeout.
   */
  @Test
  public void constructor_nullHttpOptions_appliesDefaultFiveMinuteTimeout() {
    ChatCompletionsHttpClient defaultClient =
        new ChatCompletionsHttpClient("https://example.com/", ImmutableMap.of(), null);

    OkHttpClient internal = readInternalClient(defaultClient);
    assertThat(internal.callTimeoutMillis())
        .isEqualTo((int) Duration.ofMinutes(5).toMillis()); // 300_000
  }

  /**
   * Verifies fix #6 case (b): when the caller explicitly sets {@code httpOptions.timeout() == 0},
   * the client respects this as the explicit opt-in to infinite hang. This is the migration path
   * for long-running streams or batch jobs that need no timeout.
   */
  @Test
  public void constructor_zeroTimeout_respectsInfiniteHang() {
    HttpOptions zeroTimeout = HttpOptions.builder().timeout(0).build();
    ChatCompletionsHttpClient infiniteClient =
        new ChatCompletionsHttpClient("https://example.com/", ImmutableMap.of(), zeroTimeout);

    OkHttpClient internal = readInternalClient(infiniteClient);
    assertThat(internal.callTimeoutMillis()).isEqualTo(0); // OkHttp: 0 = no timeout
  }

  /**
   * Verifies fix #6 case (c): when the caller sets a positive timeout, that value (in milliseconds)
   * is used as the call timeout.
   */
  @Test
  public void constructor_explicitTimeout_appliesIt() {
    HttpOptions tenSeconds = HttpOptions.builder().timeout(10_000).build();
    ChatCompletionsHttpClient timedClient =
        new ChatCompletionsHttpClient("https://example.com/", ImmutableMap.of(), tenSeconds);

    OkHttpClient internal = readInternalClient(timedClient);
    assertThat(internal.callTimeoutMillis()).isEqualTo(10_000);
  }

  /** Reflectively reads the internal {@link OkHttpClient} to inspect the resolved timeout. */
  private static OkHttpClient readInternalClient(ChatCompletionsHttpClient target) {
    try {
      Field clientField = ChatCompletionsHttpClient.class.getDeclaredField("client");
      clientField.setAccessible(true);
      return (OkHttpClient) clientField.get(target);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError("Failed to read internal client", e);
    }
  }
}
