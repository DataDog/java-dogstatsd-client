package com.timgroup.statsd;

/**
 * A No-Op {@link NonBlockingDirectStatsDClient}, which can be substituted in when metrics are not
 * required.
 */
public class NoOpDirectStatsDClient extends NoOpStatsDClient implements DirectStatsDClient {
    @Override public void recordDistributionValues(String aspect, double[] values, double sampleRate, String... tags) { }

    @Override public void recordDistributionValues(String aspect, long[] values, double sampleRate, String... tags) { }

    @Override public void recordSketchWithTimestamp(
        String aspect, long[] values, double sampleRate, long timestamp, String... tags) { }

}
