/*
 * Copyright 2016 Google Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.trace.zipkin;

import com.google.cloud.trace.v1.consumer.TraceConsumer;
import com.google.cloud.trace.zipkin.translation.TraceTranslator;
import com.google.devtools.cloudtrace.v1.Trace;
import com.google.devtools.cloudtrace.v1.Traces;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

/**
 * Consumes Zipkin spans, translates them to Stackdriver spans using a provided TraceTranslator, and
 * writes them to a provided Stackdriver {@link TraceConsumer}.
 */
public class StackdriverSpanConsumer implements SpanConsumer {

  private final TraceTranslator translator;
  private final TraceConsumer consumer;
  private final AsyncListenableTaskExecutor executor;

  public StackdriverSpanConsumer(
      TraceTranslator translator, TraceConsumer consumer, AsyncListenableTaskExecutor executor) {
    this.translator = translator;
    this.consumer = consumer;
    this.executor = executor;
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    ListenableFuture<Void> future =
        executor.submitListenable(
            new Callable<Void>() {
              @Override
              public Void call() {
                Collection<Trace> traces = translator.translateSpans(spans);
                consumer.receive(Traces.newBuilder().addAllTraces(traces).build());
                return null;
              }
            });
    return new ListenableFutureCall<>(future);
  }

  private static class ListenableFutureCall<V> extends Call<V> {
    private final ListenableFuture<V> future;

    private ListenableFutureCall(ListenableFuture<V> future) {
      this.future = future;
    }

    @Override
    public V execute() throws IOException {
      try {
        return future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException("Could not consume spans.", e);
      }
    }

    @Override
    public void enqueue(Callback<V> callback) {
      future.addCallback(
          new ListenableFutureCallback<V>() {
            @Override
            public void onFailure(Throwable t) {
              callback.onError(t);
            }

            @Override
            public void onSuccess(V value) {
              callback.onSuccess(value);
            }
          });
    }

    @Override
    public void cancel() {
      future.cancel(true);
    }

    @Override
    public boolean isCanceled() {
      return future.isCancelled();
    }

    @Override
    public Call<V> clone() {
      return new ListenableFutureCall<>(future);
    }
  }
}
