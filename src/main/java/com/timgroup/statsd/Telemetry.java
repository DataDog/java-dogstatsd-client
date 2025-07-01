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
    protected final String metricsByTypeSentMetric = "datadog.dogstatsd.client.metrics_by_type";
    protected final String eventsSentMetric = "datadog.dogstatsd.client.events";
    protected final String serviceChecksSentMetric = "datadog.dogstatsd.client.service_checks";
    protected final String bytesSentMetric = "datadog.dogstatsd.client.bytes_sent";
    protected final String bytesDroppedMetric = "datadog.dogstatsd.client.bytes_dropped";
    protected final String packetsSentMetric = "datadog.dogstatsd.client.packets_sent";
    protected final String packetsDroppedMetric = "datadog.dogstatsd.client.packets_dropped";
    protected final String packetsDroppedQueueMetric = "datadog.dogstatsd.client.packets_dropped_queue";
    protected final String aggregatedContextsMetric = "datadog.dogstatsd.client.aggregated_context";
    protected final String aggregatedContextsByTypeMetric = "datadog.dogstatsd.client.aggregated_context_by_type";

    protected Timer timer;
    NonBlockingStatsDClient client;

    protected class TelemetryTask extends TimerTask {
        private Telemetry telemetry;

        TelemetryTask(final Telemetry telemetry) {
            super();
            this.telemetry = telemetry;
        }

        public void run() {
            telemetry.flush();
        }
    }

    Telemetry(final NonBlockingStatsDClient client) {
        this.client = client;
    }

    /**
     * Startsthe flush timer for the telemetry.
     *
     * @param flushInterval
     *     Telemetry flush interval, in milliseconds.
     */
    public void start(final long flushInterval) {
        // flush the telemetry at regualar interval
        timer = new Timer(true);
        timer.scheduleAtFixedRate(new TelemetryTask(this), flushInterval, flushInterval);
    }

    /**
     * Stops the flush timer for the telemetry.
     */
    public void stop() {
        if (timer != null) {
            timer.cancel();
        }
    }

    /**
     * Sends Telemetry metrics to the processor. This function also reset the internal counters.
     */
    public void flush() {
        // all getAndSet will not be synchronous but it's ok since metrics will
        // be spread out among processor worker and we flush every 5s by
        // default

        client.sendTelemetryMetric(metricsSentMetric, metricsSent.getAndSet(0));
        client.sendTelemetryMetric(eventsSentMetric, eventsSent.getAndSet(0));
        client.sendTelemetryMetric(serviceChecksSentMetric, serviceChecksSent.getAndSet(0));
        client.sendTelemetryMetric(bytesSentMetric, bytesSent.getAndSet(0));
        client.sendTelemetryMetric(bytesDroppedMetric, bytesDropped.getAndSet(0));
        client.sendTelemetryMetric(packetsSentMetric, packetsSent.getAndSet(0));
        client.sendTelemetryMetric(packetsDroppedMetric, packetsDropped.getAndSet(0));
        client.sendTelemetryMetric(packetsDroppedQueueMetric, packetsDroppedQueue.getAndSet(0));
        client.sendTelemetryMetric(aggregatedContextsMetric, aggregatedContexts.getAndSet(0));

        // developer metrics
        client.sendTelemetryMetric(metricsByTypeSentMetric, gaugeSent.getAndSet(0), "metrics_type:gauge");
        client.sendTelemetryMetric(metricsByTypeSentMetric, countSent.getAndSet(0), "metrics_type:count");
        client.sendTelemetryMetric(metricsByTypeSentMetric, setSent.getAndSet(0), "metrics_type:set");
        client.sendTelemetryMetric(metricsByTypeSentMetric, histogramSent.getAndSet(0), "metrics_type:histogram");
        client.sendTelemetryMetric(metricsByTypeSentMetric, distributionSent.getAndSet(0), "metrics_type:distribution");

        client.sendTelemetryMetric(aggregatedContextsByTypeMetric, aggregatedGaugeContexts.getAndSet(0), "metrics_type:gauge");
        client.sendTelemetryMetric(aggregatedContextsByTypeMetric, aggregatedCountContexts.getAndSet(0), "metrics_type:count");
        client.sendTelemetryMetric(aggregatedContextsByTypeMetric, aggregatedSetContexts.getAndSet(0), "metrics_type:set");
    }

    /**
     * Increase Metrics Sent telemetry metric.
     *
     * @param value
     *     Value to increase metric with
     */
    public void incrMetricsSent(final int value) {
        metricsSent.addAndGet(value);
    }

    /**
     * Increase Metrics Sent telemetry metric, and specific metric type counter.
     *
     * @param value
     *     Value to increase metric with
     * @param type
     *    Message type
     */
    public void incrMetricsSent(final int value, Message.Type type) {
        incrMetricsSent(value);
        switch (type) {
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
        gaugeSent.addAndGet(value);
    }

    public void incrCountSent(final int value) {
        countSent.addAndGet(value);
    }

    public void incrHistogramSent(final int value) {
        histogramSent.addAndGet(value);
    }

    public void incrDistributionSent(final int value) {
        distributionSent.addAndGet(value);
    }

    public void incrSetSent(final int value) {
        setSent.addAndGet(value);
    }

    public void incrEventsSent(final int value) {
        eventsSent.addAndGet(value);
    }

    public void incrServiceChecksSent(final int value) {
        serviceChecksSent.addAndGet(value);
    }

    public void incrBytesSent(final int value) {
        bytesSent.addAndGet(value);
    }

    public void incrBytesDropped(final int value) {
        bytesDropped.addAndGet(value);
    }

    public void incrPacketSent(final int value) {
        packetsSent.addAndGet(value);
    }

    public void incrPacketDropped(final int value) {
        packetsDropped.addAndGet(value);
    }

    public void incrPacketDroppedQueue(final int value) {
        packetsDroppedQueue.addAndGet(value);
    }

    public void incrAggregatedContexts(final int value) {
        aggregatedContexts.addAndGet(value);
    }

    public void incrAggregatedGaugeContexts(final int value) {
        aggregatedGaugeContexts.addAndGet(value);
    }

    public void incrAggregatedCountContexts(final int value) {
        aggregatedCountContexts.addAndGet(value);
    }

    public void incrAggregatedSetContexts(final int value) {
        aggregatedSetContexts.addAndGet(value);
    }

    /**
     * Resets all counter in the telemetry (this is useful for tests purposes).
     */
    public void reset() {
        metricsSent.set(0);
        eventsSent.set(0);
        serviceChecksSent.set(0);
        bytesSent.set(0);
        bytesDropped.set(0);
        packetsSent.set(0);
        packetsDropped.set(0);
        packetsDroppedQueue.set(0);
        aggregatedContexts.set(0);

        gaugeSent.set(0);
        countSent.set(0);
        histogramSent.set(0);
        distributionSent.set(0);
        setSent.set(0);

        aggregatedGaugeContexts.set(0);
        aggregatedCountContexts.set(0);
        aggregatedSetContexts.set(0);
    }
}
