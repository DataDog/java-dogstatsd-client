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
import com.datadoghq.dogstatsd.http.serializer.TagsCardinality;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Simple Dogstatsd HTTP client for sending pre-aggregated metrics.
 *
 * <p>Two tags are given special treatment: their value is submitted as a property of the
 * timeseries.
 *
 * <ul>
 *   <li>{@code host:} — the value of the first one becomes the host resource. Every {@code host:}
 *       tag is removed from the tags.
 *   <li>{@code dd.internal.card:} — the value of the first one becomes the tags cardinality. Values
 *       the agent does not recognize ask for the cardinality the agent is configured to use. The
 *       tags are sent unchanged, so that agents that only understand the tag keep working; agents
 *       that understand the cardinality drop the tag themselves.
 * </ul>
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
    private static final String cardinalityTagPrefix = "dd.internal.card:";

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
     * @param tags the tags to attach to the point, see {@link DirectHttpClient} for the handling of
     *     special values.
     * @throws BufferOverflowException if the encoded metric exceeds the maximum payload size.
     */
    public void gauge(String name, double value, long ts, List<String> tags) {
        withTagsHostAndCardinality(seriesBuilder.gauge(prefixed(name)), tags)
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
     * @param tags the tags to attach to the point, see {@link DirectHttpClient} for the handling of
     *     special values.
     * @throws BufferOverflowException if the encoded metric exceeds the maximum payload size.
     */
    public void count(String name, double value, long ts, List<String> tags) {
        withTagsHostAndCardinality(seriesBuilder.rate(prefixed(name)), tags)
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
     * @param tags the tags to attach to the point, see {@link DirectHttpClient} for the handling of
     *     special values.
     * @throws IllegalArgumentException if {@code sampleRate} is {@code NaN}, not positive, or
     *     greater than 1.
     * @throws BufferOverflowException if the encoded metric exceeds the maximum payload size.
     */
    public void distribution(
            String name, double[] values, double sampleRate, long ts, List<String> tags) {
        sketchBuffer.build(values, sampleRate);
        withTagsHostAndCardinality(sketchesBuilder.sketch(prefixed(name)), tags)
                .addPoint(ts, sketchBuffer)
                .close();
    }

    private String prefixed(final String name) {
        return prefix.isEmpty() ? name : prefix + name;
    }

    /**
     * Applies the tags to the metric, extracting the host tag into the host resource and the
     * cardinality tag into the tags cardinality. The cardinality tag itself is kept in the tags.
     */
    private static <T extends Metric<T>> T withTagsHostAndCardinality(
            final T metric, final List<String> tags) {
        return metric.setTags(withoutHostTags(tags))
                .setResources(hostResource(hostTag(tags)))
                .setTagsCardinality(cardinality(cardinalityTag(tags)));
    }

    /** Returns the value of the first host tag, or null if there is none. */
    static String hostTag(final List<String> tags) {
        return tagValue(tags, hostTagPrefix);
    }

    /** Returns the value of the first cardinality tag, or null if there is none. */
    static String cardinalityTag(final List<String> tags) {
        return tagValue(tags, cardinalityTagPrefix);
    }

    /** Returns the value of the first tag with the given prefix, or null if there is none. */
    private static String tagValue(final List<String> tags, final String prefix) {
        if (tags == null) {
            return null;
        }
        for (int i = 0; i < tags.size(); i++) {
            final String tag = tags.get(i);
            if (tag.startsWith(prefix)) {
                return tag.substring(prefix.length());
            }
        }
        return null;
    }

    /**
     * Returns the cardinality constant matching the value of a cardinality tag. Values the agent
     * does not recognize, including null, ask for the cardinality the agent is configured to use.
     */
    static TagsCardinality cardinality(final String value) {
        if (value == null) {
            return TagsCardinality.DEFAULT;
        }
        if (value.equalsIgnoreCase("none")) {
            return TagsCardinality.NONE;
        }
        if (value.equalsIgnoreCase("low")) {
            return TagsCardinality.LOW;
        }
        if (value.equalsIgnoreCase("orchestrator") || value.equalsIgnoreCase("orch")) {
            return TagsCardinality.ORCHESTRATOR;
        }
        if (value.equalsIgnoreCase("high")) {
            return TagsCardinality.HIGH;
        }
        return TagsCardinality.DEFAULT;
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
