package com.timgroup.statsd;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class Telemetry {

    public static int DEFAULT_FLUSH_INTERVAL = 10000; // 10s

    protected AtomicInteger metricsSent;
    protected AtomicInteger eventsSent;
    protected AtomicInteger serviceChecksSent;
    protected AtomicInteger bytesSent;
    protected AtomicInteger bytesDropped;
    protected AtomicInteger packetsSent;
    protected AtomicInteger packetsDropped;
    protected AtomicInteger packetsDroppedQueue;

    protected String metricsSentMetric;
    protected String eventsSentMetric;
    protected String serviceChecksSentMetric;
    protected String bytesSentMetric;
    protected String bytesDroppedMetric;
    protected String packetsSentMetric;
    protected String packetsDroppedMetric;
    protected String packetsDroppedQueueMetric;

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

    Telemetry(final String tags, final StatsDProcessor processor) {
        // precompute metrics lines with tags
        this.metricsSentMetric = "datadog.dogstatsd.client.metrics:%d|c" + tags;
        this.eventsSentMetric = "datadog.dogstatsd.client.events:%d|c" + tags;
        this.serviceChecksSentMetric = "datadog.dogstatsd.client.service_checks:%d|c" + tags;
        this.bytesSentMetric = "datadog.dogstatsd.client.bytes_sent:%d|c" + tags;
        this.bytesDroppedMetric = "datadog.dogstatsd.client.bytes_dropped:%d|c" + tags;
        this.packetsSentMetric = "datadog.dogstatsd.client.packets_sent:%d|c" + tags;
        this.packetsDroppedMetric = "datadog.dogstatsd.client.packets_dropped:%d|c" + tags;
        this.packetsDroppedQueueMetric = "datadog.dogstatsd.client.packets_dropped_queue:%d|c" + tags;

        this.metricsSent = new AtomicInteger(0);
        this.eventsSent = new AtomicInteger(0);
        this.serviceChecksSent = new AtomicInteger(0);
        this.bytesSent = new AtomicInteger(0);
        this.bytesDropped = new AtomicInteger(0);
        this.packetsSent = new AtomicInteger(0);
        this.packetsDropped = new AtomicInteger(0);
        this.packetsDroppedQueue = new AtomicInteger(0);

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
        this.timer = new Timer();
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

        this.processor.send(String.format(this.metricsSentMetric, this.metricsSent.getAndSet(0)));
        this.processor.send(String.format(this.eventsSentMetric, this.eventsSent.getAndSet(0)));
        this.processor.send(String.format(this.serviceChecksSentMetric, this.serviceChecksSent.getAndSet(0)));
        this.processor.send(String.format(this.bytesSentMetric, this.bytesSent.getAndSet(0)));
        this.processor.send(String.format(this.bytesDroppedMetric, this.bytesDropped.getAndSet(0)));
        this.processor.send(String.format(this.packetsSentMetric, this.packetsSent.getAndSet(0)));
        this.processor.send(String.format(this.packetsDroppedMetric, this.packetsDropped.getAndSet(0)));
        this.processor.send(String.format(this.packetsDroppedQueueMetric, this.packetsDroppedQueue.getAndSet(0)));
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
}
