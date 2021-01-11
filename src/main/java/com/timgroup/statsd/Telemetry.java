package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class Telemetry {

    public static int DEFAULT_FLUSH_INTERVAL = 10000; // 10s

    protected final AtomicInteger metricsSent = new AtomicInteger(0);
    protected final AtomicInteger gaugeSent = new AtomicInteger(0);
    protected final AtomicInteger countSent = new AtomicInteger(0);
    protected final AtomicInteger histogramSent = new AtomicInteger(0);
    protected final AtomicInteger distributionSent = new AtomicInteger(0);
    protected final AtomicInteger setSent = new AtomicInteger(0);
    protected final AtomicInteger eventsSent = new AtomicInteger(0);
    protected final AtomicInteger serviceChecksSent = new AtomicInteger(0);
    protected final AtomicInteger bytesSent = new AtomicInteger(0);
    protected final AtomicInteger bytesDropped = new AtomicInteger(0);
    protected final AtomicInteger packetsSent = new AtomicInteger(0);
    protected final AtomicInteger packetsDropped = new AtomicInteger(0);
    protected final AtomicInteger packetsDroppedQueue = new AtomicInteger(0);
    protected final AtomicInteger aggregatedContexts = new AtomicInteger(0);
    protected final AtomicInteger aggregatedGaugeContexts = new AtomicInteger(0);
    protected final AtomicInteger aggregatedCountContexts = new AtomicInteger(0);
    protected final AtomicInteger aggregatedSetContexts = new AtomicInteger(0);

    protected final String metricsSentMetric = "datadog.dogstatsd.client.metrics";
    protected final String gaugeSentMetric = "datadog.dogstatsd.client.metricsGauge";
    protected final String countSentMetric = "datadog.dogstatsd.client.metricsCount";
    protected final String histogramSentMetric = "datadog.dogstatsd.client.metricsHistogram";
    protected final String distributionSentMetric = "datadog.dogstatsd.client.metricsDistribution";
    protected final String setSentMetric = "datadog.dogstatsd.client.metricsSet";
    protected final String eventsSentMetric = "datadog.dogstatsd.client.events";
    protected final String serviceChecksSentMetric = "datadog.dogstatsd.client.service_checks";
    protected final String bytesSentMetric = "datadog.dogstatsd.client.bytes_sent";
    protected final String bytesDroppedMetric = "datadog.dogstatsd.client.bytes_dropped";
    protected final String packetsSentMetric = "datadog.dogstatsd.client.packets_sent";
    protected final String packetsDroppedMetric = "datadog.dogstatsd.client.packets_dropped";
    protected final String packetsDroppedQueueMetric = "datadog.dogstatsd.client.packets_dropped_queue";
    protected final String aggregatedContextsMetric = "datadog.dogstatsd.client.aggregated_context";
    protected final String aggregatedGaugeContextsMetric = "datadog.dogstatsd.client.aggregated_context_gauge";
    protected final String aggregatedCountContextsMetric = "datadog.dogstatsd.client.aggregated_context_count";
    protected final String aggregatedSetContextsMetric = "datadog.dogstatsd.client.aggregated_context_set";

    protected String tags;
    protected boolean devMode;

    public StatsDProcessor processor;
    protected Timer timer;

    protected class TelemetryTask extends TimerTask {
        private Telemetry telemetry;

        TelemetryTask(final Telemetry telemetry) {
            super();
            this.telemetry = telemetry;
        }

        public void run() {
            this.telemetry.flush();
        }
    }

    class TelemetryMessage extends NumericMessage<Integer> {
        private final String tagsString;  // pre-baked comma separeated tags string

        protected TelemetryMessage(String metric, Integer value, String tags) {
            super(metric, Message.Type.COUNT, value, null);
            this.tagsString = tags;
            this.done = true;  // dont aggregate telemetry messages for now
        }

        @Override
        public final void writeTo(StringBuilder builder) {
            builder.append(aspect)
                .append(':')
                .append(this.value)
                .append('|')
                .append(type)
                .append(tagsString)
                .append('\n');  // already has the statsd separator baked-in
        }
    }

    Telemetry(final String tags, final StatsDProcessor processor, final boolean devMode) {
        // precompute metrics lines with tags
        this.tags = tags;
        this.processor = processor;
        this.devMode = devMode;
        this.timer = null;
    }

    public static class Builder {
        private String tags;
        private StatsDProcessor processor;
        private boolean devMode;

        public Builder() {}

        public Builder tags(String tags) {
            this.tags = tags;
            return this;
        }

        public Builder processor(StatsDProcessor processor) {
            this.processor = processor;
            return this;
        }

        public Builder devMode(boolean devMode) {
            this.devMode = devMode;
            return this;
        }
        public Telemetry build() {
            return new Telemetry(this.tags, this.processor, this.devMode);
        }
    }

    /**
     * Startsthe flush timer for the telemetry.
     *
     * @param flushInterval
     *     Telemetry flush interval, in milliseconds.
     */
    public void start(final long flushInterval) {
        // flush the telemetry at regualar interval
        this.timer = new Timer(true);
        this.timer.scheduleAtFixedRate(new TelemetryTask(this), flushInterval, flushInterval);
    }

    /**
     * Stops the flush timer for the telemetry.
     */
    public void stop() {
        if (this.timer != null) {
            this.timer.cancel();
        }
    }

    /**
     * Sends Telemetry metrics to the processor. This function also reset the internal counters.
     */
    public void flush() {
        // all getAndSet will not be synchronous but it's ok since metrics will
        // be spread out among processor worker and we flush every 5s by
        // default

        processor.send(new TelemetryMessage(this.metricsSentMetric, this.metricsSent.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.eventsSentMetric, this.eventsSent.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.serviceChecksSentMetric, this.serviceChecksSent.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.bytesSentMetric, this.bytesSent.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.bytesDroppedMetric, this.bytesDropped.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.packetsSentMetric, this.packetsSent.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.packetsDroppedMetric, this.packetsDropped.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.packetsDroppedQueueMetric, this.packetsDroppedQueue.getAndSet(0), tags));
        processor.send(new TelemetryMessage(this.aggregatedContextsMetric, this.aggregatedContexts.getAndSet(0), tags));

        if (devMode) {
            processor.send(new TelemetryMessage(this.gaugeSentMetric, this.gaugeSent.getAndSet(0), tags));
            processor.send(new TelemetryMessage(this.countSentMetric, this.countSent.getAndSet(0), tags));
            processor.send(new TelemetryMessage(this.histogramSentMetric, this.histogramSent.getAndSet(0), tags));
            processor.send(new TelemetryMessage(this.distributionSentMetric, this.distributionSent.getAndSet(0), tags));
            processor.send(new TelemetryMessage(this.setSentMetric, this.setSent.getAndSet(0), tags));

            processor.send(new TelemetryMessage(this.aggregatedGaugeContextsMetric, this.aggregatedGaugeContexts.getAndSet(0), tags));
            processor.send(new TelemetryMessage(this.aggregatedCountContextsMetric, this.aggregatedCountContexts.getAndSet(0), tags));
            processor.send(new TelemetryMessage(this.aggregatedSetContextsMetric, this.aggregatedSetContexts.getAndSet(0), tags));
        }
    }

    public void incrMetricsSent(final int value) {
        this.metricsSent.addAndGet(value);
    }

    public void incrMetricsSent(final int value, Message.Type type) {
        incrMetricsSent(value);
        switch(type) {
                case GAUGE:
                    incrGaugeSent(value);
                    break;
                case COUNT:
                    incrCountSent(value);
                    break;
                case SET:
                    incrSetSent(value);
                    break;
                case HISTOGRAM:
                    incrHistogramSent(value);
                    break;
                case DISTRIBUTION:
                    incrDistributionSent(value);
                    break;
                default:
                    break;
        }
    }

    public void incrGaugeSent(final int value) {
        this.gaugeSent.addAndGet(value);
    }

    public void incrCountSent(final int value) {
        this.countSent.addAndGet(value);
    }


    public void incrHistogramSent(final int value) {
        this.histogramSent.addAndGet(value);
    }


    public void incrDistributionSent(final int value) {
        this.distributionSent.addAndGet(value);
    }

    public void incrSetSent(final int value) {
        this.setSent.addAndGet(value);
    }

    public void incrEventsSent(final int value) {
        this.eventsSent.addAndGet(value);
    }

    public void incrServiceChecksSent(final int value) {
        this.serviceChecksSent.addAndGet(value);
    }

    public void incrBytesSent(final int value) {
        this.bytesSent.addAndGet(value);
    }

    public void incrBytesDropped(final int value) {
        this.bytesDropped.addAndGet(value);
    }

    public void incrPacketSent(final int value) {
        this.packetsSent.addAndGet(value);
    }

    public void incrPacketDropped(final int value) {
        this.packetsDropped.addAndGet(value);
    }

    public void incrPacketDroppedQueue(final int value) {
        this.packetsDroppedQueue.addAndGet(value);
    }

    public void incrAggregatedContexts(final int value) {
        this.aggregatedContexts.addAndGet(value);
    }

    public void incrAggregatedGaugeContexts(final int value) {
        this.aggregatedGaugeContexts.addAndGet(value);
    }

    public void incrAggregatedCountContexts(final int value) {
        this.aggregatedCountContexts.addAndGet(value);
    }

    public void incrAggregatedSetContexts(final int value) {
        this.aggregatedSetContexts.addAndGet(value);
    }

    /**
     * Resets all counter in the telemetry (this is useful for tests purposes).
     */
    public void reset() {
        this.metricsSent.set(0);
        this.eventsSent.set(0);
        this.serviceChecksSent.set(0);
        this.bytesSent.set(0);
        this.bytesDropped.set(0);
        this.packetsSent.set(0);
        this.packetsDropped.set(0);
        this.packetsDroppedQueue.set(0);
        this.aggregatedContexts.set(0);

        // if (devMode) {
        this.gaugeSent.set(0);
        this.countSent.set(0);
        this.histogramSent.set(0);
        this.distributionSent.set(0);
        this.setSent.set(0);

        this.aggregatedGaugeContexts.set(0);
        this.aggregatedCountContexts.set(0);
        this.aggregatedSetContexts.set(0);
        // }
    }

    /**
     * Gets the telemetry tags string.
     * @return this Telemetry instance applied tags.
     */
    public String getTags() {
        return this.tags;
    }

    /**
     * Gets the dev mode setting value.
     * @return this Telemetry instance dev mode setting.
     */
    public boolean getDevMode() {
        return this.devMode;
    }
}
