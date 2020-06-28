package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class Telemetry {

    public static int DEFAULT_FLUSH_INTERVAL = 10000; // 10s

    protected final AtomicInteger metricsSent = new AtomicInteger(0);

    protected final AtomicInteger eventsSent = new AtomicInteger(0);
    protected final AtomicInteger serviceChecksSent = new AtomicInteger(0);
    protected final AtomicInteger bytesSent = new AtomicInteger(0);
    protected final AtomicInteger bytesDropped = new AtomicInteger(0);
    protected final AtomicInteger packetsSent = new AtomicInteger(0);
    protected final AtomicInteger packetsDropped = new AtomicInteger(0);
    protected final AtomicInteger packetsDroppedQueue = new AtomicInteger(0);

    protected final String metricsSentMetric = "datadog.dogstatsd.client.metrics";
    protected final String eventsSentMetric = "datadog.dogstatsd.client.events";
    protected final String serviceChecksSentMetric = "datadog.dogstatsd.client.service_checks";
    protected final String bytesSentMetric = "datadog.dogstatsd.client.bytes_sent";
    protected final String bytesDroppedMetric = "datadog.dogstatsd.client.bytes_dropped";
    protected final String packetsSentMetric = "datadog.dogstatsd.client.packets_sent";
    protected final String packetsDroppedMetric = "datadog.dogstatsd.client.packets_dropped";
    protected final String packetsDroppedQueueMetric = "datadog.dogstatsd.client.packets_dropped_queue";

    protected String tags;

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

    class TelemetryMessage extends Message {
        private final String tags;  // pre-baked comma separeated tags string

        protected TelemetryMessage(String metric, int value, String tags) {
            super(metric, Message.Type.COUNT, value);
            this.tags = tags;
        }

        @Override
        public final void writeTo(StringBuilder builder) {
            builder.append(aspect)
                .append(':')
                .append(this.value)
                .append('|')
                .append(type)
                .append(tags);  // already has the statsd separator baked-in
        }
    }

    Telemetry(final String tags, final StatsDProcessor processor) {
        // precompute metrics lines with tags
        this.tags = tags;
        this.processor = processor;
        this.timer = null;
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
    }

    public void incrMetricsSent(final int value) {
        this.metricsSent.addAndGet(value);
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
    }

    /**
     * Gets the telemetry tags string.
     * @return this Telemetry instance applied tags.
     */
    public String getTags() {
        return this.tags;
    }
}
