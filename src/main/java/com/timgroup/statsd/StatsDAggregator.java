package com.timgroup.statsd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class StatsDAggregator {
    public static int DEFAULT_FLUSH_INTERVAL = 2000; // 2s
    public static int DEFAULT_SHARDS = 4; // 4 partitions to reduce contention.

    protected final String AGGREGATOR_THREAD_NAME = "statsd-aggregator-thread";
    protected final ArrayList<Map<Message, Message>> aggregateMetrics;

    protected final int shardGranularity;
    protected final long flushInterval;

    private final StatsDProcessor processor;

    protected Timer scheduler = null;

    private Telemetry telemetry;

    private class FlushTask extends TimerTask {
        @Override
        public void run() {
            flush();
        }
    }

    /**
     * StatsDAggregtor constructor.
     *
     * @param processor the message processor, aggregated messages will be queued in the high
     *     priority queue.
     * @param shards number of shards for the aggregation map.
     * @param flushInterval flush interval in miliseconds, 0 disables message aggregation.
     */
    public StatsDAggregator(
            final StatsDProcessor processor, final int shards, final long flushInterval) {
        this.processor = processor;
        this.flushInterval = flushInterval;
        this.shardGranularity = shards;
        this.aggregateMetrics = new ArrayList<>(shards);

        if (flushInterval > 0) {
            this.scheduler = new Timer(AGGREGATOR_THREAD_NAME, true);
        }

        for (int i = 0; i < this.shardGranularity; i++) {
            this.aggregateMetrics.add(i, new HashMap<Message, Message>());
        }
    }

    /** Start the aggregator flushing scheduler. */
    public void start() {
        if (flushInterval > 0) {
            // snapshot of processor telemetry - avoid volatile reference to harness CPU cache
            // caller responsible of setting telemetry before starting
            telemetry = processor.getTelemetry();
            scheduler.scheduleAtFixedRate(new FlushTask(), flushInterval, flushInterval);
        }
    }

    /** Stop the aggregator flushing scheduler. */
    public void stop() {
        if (flushInterval > 0) {
            scheduler.cancel();
        }
    }

    /**
     * Aggregate a message if possible.
     *
     * @param message the dogstatsd Message we wish to aggregate.
     * @return a boolean reflecting if the message was aggregated.
     */
    public boolean aggregateMessage(Message message) {
        if (flushInterval == 0 || !message.canAggregate() || message.getDone()) {
            return false;
        }

        int hash = message.hashCode();
        int bucket = Math.abs(hash % this.shardGranularity);
        Map<Message, Message> map = aggregateMetrics.get(bucket);

        synchronized (map) {
            // For now let's just put the message in the map
            Message msg = MapUtils.putIfAbsent(map, message);
            if (msg != null) {
                msg.aggregate(message);
                if (telemetry != null) {
                    telemetry.incrAggregatedContexts(1);

                    // developer metrics
                    switch (message.getType()) {
                        case GAUGE:
                            telemetry.incrAggregatedGaugeContexts(1);
                            break;
                        case COUNT:
                            telemetry.incrAggregatedCountContexts(1);
                            break;
                        case SET:
                            telemetry.incrAggregatedSetContexts(1);
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        return true;
    }

    public final long getFlushInterval() {
        return this.flushInterval;
    }

    public final int getShardGranularity() {
        return this.shardGranularity;
    }

    protected void flush() {
        for (int i = 0; i < shardGranularity; i++) {
            Map<Message, Message> map = aggregateMetrics.get(i);

            synchronized (map) {
                Iterator<Map.Entry<Message, Message>> iter = map.entrySet().iterator();
                while (iter.hasNext()) {
                    Message msg = iter.next().getValue();
                    msg.setDone(true);

                    if (!processor.sendHighPrio(msg) && (telemetry != null)) {
                        telemetry.incrPacketDroppedQueue(1);
                    }

                    iter.remove();
                }
            }
        }
    }
}
