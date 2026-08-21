/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http;

import com.datadoghq.dogstatsd.Sketch;
import com.datadoghq.dogstatsd.http.serializer.Metric;
import com.datadoghq.dogstatsd.http.serializer.PayloadBuilder;
import com.datadoghq.dogstatsd.http.serializer.PayloadConsumer;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Simple Dogstatsd HTTP client for sending pre-aggregated metrics.
 *
 * <p>A {@code host:} tag is not sent as a tag: it is removed from the tags and submitted as the
 * host resource of the timeseries.
 *
 * <p>Not thread safe.
 *
 * <p>Caveat: if the forwarder throws {@code InterruptedException}, the payload in progress is lost.
 */
public class DirectHttpClient {
    private static URI seriesUri = URI.create("series");
    private static URI sketchesUri = URI.create("sketches");
    private final PayloadBuilder seriesBuilder;
    private final PayloadBuilder sketchesBuilder;
    private final Sketch sketchBuffer = new Sketch();
    private final String prefix;
    private static final int defaultInterval = 10;
    private static final String hostTagPrefix = "host:";
    private static final String hostResourceType = "host";

    /**
     * Creates a builder for a client sending its payloads through the given forwarder.
     *
     * @param forwarder the forwarder used to send payloads, required.
     * @return a new builder.
     * @throws NullPointerException if {@code forwarder} is null.
     */
    public static Builder builder(final Forwarder forwarder) {
        return new Builder(forwarder);
    }

    private DirectHttpClient(final Builder builder) {
        final Forwarder forwarder = builder.forwarder;

        if (builder.prefix != null && !builder.prefix.isEmpty()) {
            prefix = builder.prefix + ".";
        } else {
            prefix = "";
        }

        seriesBuilder =
                new PayloadBuilder(
                        new PayloadConsumer() {
                            @Override
                            public void handle(byte[] payload) {
                                try {
                                    forwarder.send(seriesUri, payload);
                                } catch (InterruptedException ex) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        });
        sketchesBuilder =
                new PayloadBuilder(
                        new PayloadConsumer() {
                            @Override
                            public void handle(byte[] payload) {
                                try {
                                    forwarder.send(sketchesUri, payload);
                                } catch (InterruptedException ex) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        });
    }

    /** Sends the payloads produced by a {@link DirectHttpClient} to their destination. */
    public static interface Forwarder {
        /**
         * Sends a single payload.
         *
         * @param uri the endpoint the payload is destined for, relative to the forwarder's base
         *     URI.
         * @param payload the encoded payload.
         * @throws InterruptedException if the calling thread is interrupted while sending.
         */
        void send(URI uri, byte[] payload) throws InterruptedException;
    }

    /** Builds a {@link DirectHttpClient}. Obtained via {@link DirectHttpClient#builder}. */
    public static class Builder {
        private final Forwarder forwarder;
        private String prefix;

        private Builder(final Forwarder forwarder) {
            this.forwarder = Objects.requireNonNull(forwarder, "forwarder");
        }

        /**
         * Sets the prefix to apply to the names of the metrics sent via this client. The prefix is
         * separated from the metric name by a dot.
         *
         * @param val the prefix, or null for no prefix.
         * @return this builder.
         */
        public Builder prefix(final String val) {
            prefix = val;
            return this;
        }

        /**
         * Builds the client.
         *
         * @return a new client.
         */
        public DirectHttpClient build() {
            return new DirectHttpClient(this);
        }
    }

    /**
     * Records a gauge point.
     *
     * @param name the metric name, to which the client prefix is prepended.
     * @param value the gauge value.
     * @param ts the timestamp of the point in seconds since Unix epoch.
     * @param tags the tags to attach to the point. A {@code host:} tag is not attached as a tag:
     *     the first one is submitted as the host resource of the timeseries, and any further {@code
     *     host:} tags are dropped.
     * @throws BufferOverflowException if the encoded metric exceeds the maximum payload size.
     */
    public void gauge(String name, double value, long ts, List<String> tags) {
        withTagsAndHost(seriesBuilder.gauge(prefixed(name)), tags)
                .setInterval(defaultInterval)
                .addPoint(ts, value)
                .close();
    }

    /**
     * Records a count point.
     *
     * <p>For compatibility with aggregated dogstatsd counts, assumes an aggregation interval of
     * 10s.
     *
     * @param name the metric name, to which the client prefix is prepended.
     * @param value the count accumulated over the interval starting at {@code ts}.
     * @param ts the timestamp of the point in seconds since Unix epoch.
     * @param tags the tags to attach to the point. A {@code host:} tag is not attached as a tag:
     *     the first one is submitted as the host resource of the timeseries, and any further {@code
     *     host:} tags are dropped.
     * @throws BufferOverflowException if the encoded metric exceeds the maximum payload size.
     */
    public void count(String name, double value, long ts, List<String> tags) {
        withTagsAndHost(seriesBuilder.rate(prefixed(name)), tags)
                .setInterval(defaultInterval)
                .addPoint(ts, value / defaultInterval)
                .close();
    }

    /**
     * Records a distribution point summarizing the given observations as a sketch.
     *
     * @param name the metric name, to which the client prefix is prepended.
     * @param values the observations to summarize.
     * @param sampleRate the sampling rate used to collect {@code values}, in {@code (0, 1]}.
     * @param ts the timestamp of the point in seconds since Unix epoch.
     * @param tags the tags to attach to the point. A {@code host:} tag is not attached as a tag:
     *     the first one is submitted as the host resource of the timeseries, and any further {@code
     *     host:} tags are dropped.
     * @throws IllegalArgumentException if {@code sampleRate} is {@code NaN}, not positive, or
     *     greater than 1.
     * @throws BufferOverflowException if the encoded metric exceeds the maximum payload size.
     */
    public void distribution(
            String name, double[] values, double sampleRate, long ts, List<String> tags) {
        sketchBuffer.build(values, sampleRate);
        withTagsAndHost(sketchesBuilder.sketch(prefixed(name)), tags)
                .addPoint(ts, sketchBuffer)
                .close();
    }

    private String prefixed(final String name) {
        return prefix.isEmpty() ? name : prefix + name;
    }

    /** Applies the tags to the metric, extracting the host tag into the host resource. */
    private static <T extends Metric<T>> T withTagsAndHost(
            final T metric, final List<String> tags) {
        final String host = hostTag(tags);
        return metric.setTags(host == null ? tags : withoutHostTags(tags))
                .setResources(hostResource(host));
    }

    /** Returns the value of the first host tag, or null if there is none. */
    static String hostTag(final List<String> tags) {
        if (tags == null) {
            return null;
        }
        for (int i = 0; i < tags.size(); i++) {
            final String tag = tags.get(i);
            if (tag.startsWith(hostTagPrefix)) {
                return tag.substring(hostTagPrefix.length());
            }
        }
        return null;
    }

    /** Returns the tags with every host tag removed, or the tags themselves if there was none. */
    static List<String> withoutHostTags(final List<String> tags) {
        if (tags == null) {
            return null;
        }
        ArrayList<String> rest = null;
        for (int i = 0; i < tags.size(); i++) {
            final String tag = tags.get(i);
            if (tag.startsWith(hostTagPrefix)) {
                if (rest == null) {
                    rest = new ArrayList<>(tags.subList(0, i));
                }
            } else if (rest != null) {
                rest.add(tag);
            }
        }
        return rest == null ? tags : rest;
    }

    /** Returns the host resource pair, or null if there is no host. */
    static List<String> hostResource(final String host) {
        return host == null ? null : Arrays.asList(hostResourceType, host);
    }

    /**
     * Completes any in-progress payloads and submits them to the forwarder.
     *
     * @throws BufferOverflowException if an encoded metric exceeds the maximum payload size.
     */
    public void flush() {
        seriesBuilder.close();
        sketchesBuilder.close();
    }
}
