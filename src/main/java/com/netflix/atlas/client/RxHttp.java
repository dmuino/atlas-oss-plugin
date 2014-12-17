/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.atlas.client;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.timeout.ReadTimeoutException;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.pipeline.ssl.DefaultFactories;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.exceptions.CompositeException;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

final class RxHttp {
    private static final JsonFactory SMILE_FACTORY = new SmileFactory();
    private static final Logger LOGGER = LoggerFactory.getLogger(RxHttp.class);
    private static final long RETRY_DELAY_MS = 500;
    private static final double NUM_RETRIES = 3;
    private static final int HTTP_TOO_MANY = 429;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;
    private static final int HTTPS_PORT = 443;
    private static final int HTTP_PORT = 80;
    private static final int HTTP_SERVER_ERRRORS = 500;

    private static final int MIN_COMPRESS_SIZE = 512;
    private static final int READ_TIMEOUT_MS = 10000;
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final String USER_AGENT = "AtlasRxHttp";

    private RxHttp() {
    }

    static rx.Observable<HttpClientResponse<ByteBuf>>
    postSmile(String uriStr, JsonPayload payload) {
        byte[] entity = toByteArray(SMILE_FACTORY, payload);
        URI uri = URI.create(uriStr);
        return post(uri, "application/x-jackson-smile", entity);
    }

    static byte[] toByteArray(JsonFactory factory, JsonPayload payload) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            JsonGenerator gen = factory.createJsonGenerator(baos, JsonEncoding.UTF8);
            payload.toJson(gen);
            gen.close();
            baos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void logErr(String prefix, Throwable e, int sent, int total) {
        if (LOGGER.isWarnEnabled()) {
            final Throwable cause = e.getCause() != null ? e.getCause() : e;
            String msg = String.format("%s exception %s:%s Sent %d/%d",
                    prefix,
                    cause.getClass().getSimpleName(), cause.getMessage(),
                    sent, total);
            LOGGER.warn(msg);
            if (cause instanceof CompositeException) {
                CompositeException ce = (CompositeException) cause;
                for (Throwable t : ce.getExceptions()) {
                    LOGGER.warn(" Exception {}: {}", t.getClass().getSimpleName(), t.getMessage());
                }
            }
        }
    }

    static int sendAll(Iterable<Observable<Integer>> batches, final int numMetrics, long timeoutMillis) {
        final AtomicBoolean err = new AtomicBoolean(false);
        final AtomicInteger updated = new AtomicInteger(0);
        LOGGER.debug("Got {} ms to send {} metrics", timeoutMillis, numMetrics);
        try {
            final CountDownLatch completed = new CountDownLatch(1);
            final Subscription s = Observable.mergeDelayError(Observable.from(batches))
                    .timeout(timeoutMillis, TimeUnit.MILLISECONDS)
                    .subscribeOn(Schedulers.immediate())
                    .subscribe(new Action1<Integer>() {
                        @Override
                        public void call(Integer batchSize) {
                            updated.addAndGet(batchSize);
                        }
                    }, new Action1<Throwable>() {
                        @Override
                        public void call(Throwable exc) {
                            logErr("onError caught", exc, updated.get(), numMetrics);
                            err.set(true);
                            completed.countDown();
                        }
                    }, new Action0() {
                        @Override
                        public void call() {
                            completed.countDown();
                        }
                    });
            try {
                completed.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                err.set(true);
                s.unsubscribe();
                LOGGER.warn("Timed out sending metrics. {}/{} sent", updated.get(), numMetrics);
            }
        } catch (Exception e) {
            err.set(true);
            logErr("Unexpected ", e, updated.get(), numMetrics);
        }

        if (updated.get() < numMetrics && !err.get()) {
            LOGGER.warn("No error caught, but only {}/{} sent.", updated.get(), numMetrics);
        }
        return updated.get();
    }

    /**
     * Return the port taking care of handling defaults for http and https if not explicit in the
     * uri.
     */
    static int getPort(URI uri) {
        final int defaultPort = ("https".equals(uri.getScheme())) ? HTTPS_PORT : HTTP_PORT;
        return (uri.getPort() <= 0) ? defaultPort : uri.getPort();
    }

    private static Server getServerForUri(URI uri) {
        final boolean secure = "https".equals(uri.getScheme());
        return new Server(uri.getHost(), getPort(uri), secure);
    }

    /**
     * Create relative uri string with the path and query.
     */
    private static String relative(URI uri) {
        String r = uri.getRawPath();
        if (uri.getRawQuery() != null) {
            r += "?" + uri.getRawQuery();
        }
        return r;
    }


    private static HttpClientRequest<ByteBuf> compress(
            HttpClientRequest<ByteBuf> req, byte[] entity) {
        if (entity.length >= MIN_COMPRESS_SIZE) {
            req.withHeader(HttpHeaders.Names.CONTENT_ENCODING, HttpHeaders.Values.GZIP);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
                gzip.write(entity);
            } catch (IOException e) {
                // This isn't expected to occur
                throw new RuntimeException("failed to gzip request payload", e);
            }
            req.withContent(baos.toByteArray());
        } else {
            req.withContent(entity);
        }
        return req;
    }

    /**
     * Execute an HTTP request.
     *
     * @param server Server to send the request to.
     * @param req    Request to execute.
     * @return Observable with the response of the request.
     */
    private static Observable<HttpClientResponse<ByteBuf>>
    executeSingle(Server server, HttpClientRequest<ByteBuf> req) {
        HttpClient.HttpClientConfig config = new HttpClient.HttpClientConfig.Builder()
                .readTimeout(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .userAgent(USER_AGENT)
                .build();


        HttpClientBuilder<ByteBuf, ByteBuf> builder =
                RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(server.host(), server.port())
                        .pipelineConfigurator(PipelineConfigurators.<ByteBuf, ByteBuf>httpClientConfigurator())
                        .config(config)
                        .channelOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS);

        if (server.isSecure()) {
            builder.withSslEngineFactory(DefaultFactories.trustAll());
        }

        final HttpClient<ByteBuf, ByteBuf> client = builder.build();
        return client.submit(req)
                .doOnNext(new Action1<HttpClientResponse<ByteBuf>>() {
                    @Override
                    public void call(HttpClientResponse<ByteBuf> res) {
                        LOGGER.debug("Got response: {}", res.getStatus().code());
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        LOGGER.info("Error sending metrics: {}/{}",
                                throwable.getClass().getSimpleName(),
                                throwable.getMessage());
                    }
                })
                .doOnTerminate(new Action0() {
                    @Override
                    public void call() {
                        client.shutdown();
                    }
                });
    }

    /**
     * Perform a POST request.
     *
     * @param uri         Location to send the data.
     * @param contentType MIME type for the request payload.
     * @param entity      Data to send.
     * @return Observable with the response of the request.
     */
    static Observable<HttpClientResponse<ByteBuf>>
    post(URI uri, String contentType, byte[] entity) {
        Server server = getServerForUri(uri);
        HttpClientRequest<ByteBuf> req = HttpClientRequest.createPost(relative(uri))
                .withHeader(HttpHeaders.Names.CONTENT_TYPE, contentType);
        return execute(server, compress(req, entity));
    }

    private static long getRetryDelay(HttpClientResponse<ByteBuf> res, long dflt) {
        try {
            if (res.getHeaders().contains(HttpHeaders.Names.RETRY_AFTER)) {
                // http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.37
                int delaySeconds = res.getHeaders().getIntHeader(HttpHeaders.Names.RETRY_AFTER);
                return TimeUnit.MILLISECONDS.convert(delaySeconds, TimeUnit.SECONDS);
            }
        } catch (NumberFormatException e) {
            // We don't support the date version, so use dflt in this case
            return dflt;
        }
        return dflt;
    }


    /**
     * Execute an HTTP request.
     *
     * @param server Server to use. The request will be retried a max of 3 times until a successful
     *               response or a non-retriable error occurs. For status codes 429 and 503 the
     *               {@code Retry-After} header is honored. Otherwise an exponential back-off will
     *               be used.
     * @param req    Request to execute.
     * @return Observable with the response of the request.
     */
    static Observable<HttpClientResponse<ByteBuf>>
    execute(final Server server, final HttpClientRequest<ByteBuf> req) {
        req.withHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

        final long backoffMillis = RETRY_DELAY_MS;
        Observable<HttpClientResponse<ByteBuf>> observable = executeSingle(server, req);
        for (int i = 1; i < NUM_RETRIES; ++i) {
            final long delay = backoffMillis << (i - 1);
            observable = observable
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> res) {
                            final int code = res.getStatus().code();
                            Observable<HttpClientResponse<ByteBuf>> resObs;
                            if (code == HTTP_TOO_MANY || code == HTTP_SERVICE_UNAVAILABLE) {
                                final long retryDelay = getRetryDelay(res, delay);
                                res.getContent().subscribe();
                                resObs = executeSingle(server, req);
                                if (retryDelay > 0) {
                                    resObs = resObs.delaySubscription(retryDelay, TimeUnit.MILLISECONDS);
                                }
                            } else if (code >= HTTP_SERVER_ERRRORS) {
                                res.getContent().subscribe();
                                resObs = executeSingle(server, req);
                            } else {
                                resObs = Observable.just(res);
                            }
                            return resObs;
                        }
                    })
                    .onErrorResumeNext(new Func1<Throwable, Observable<? extends HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<? extends HttpClientResponse<ByteBuf>> call(Throwable throwable) {
                            if (throwable instanceof ConnectException
                                    || throwable instanceof ReadTimeoutException) {
                                return executeSingle(server, req);
                            }
                            return Observable.error(throwable);
                        }
                    });
        }

        return observable;
    }


    /**
     * Represents a server to try and connect to.
     */
    private static class Server {
        private final String host;
        private final int port;
        private final boolean secure;

        /**
         * Create a new instance.
         */
        Server(String host, int port, boolean secure) {
            this.host = host;
            this.port = port;
            this.secure = secure;
        }

        /**
         * Return the host name for the server.
         */
        public String host() {
            return host;
        }

        /**
         * Return the port for the server.
         */
        public int port() {
            return port;
        }

        /**
         * Return true if HTTPS should be used.
         */
        public boolean isSecure() {
            return secure;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Server server = (Server) o;
            return port == server.port && secure == server.secure && host.equals(server.host);
        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + port;
            result = 31 * result + (secure ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Server{"
                    + "host='" + host + '\''
                    + ", port=" + port
                    + ", secure=" + secure
                    + '}';
        }
    }
}
