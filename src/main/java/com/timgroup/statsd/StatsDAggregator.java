package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;


public class StatsDAggregator {
    public static int DEFAULT_FLUSH_INTERVAL = 15000; // 15s
    public static int DEFAULT_SHARDS = 4;  // 4 partitions to reduce contention.

    protected final String AGGREGATOR_THREAD_NAME = "statsd-aggregator-thread";
    protected final Set<Message.Type> aggregateSet = new HashSet<>(
            Arrays.asList(Message.Type.COUNT, Message.Type.GAUGE));
    protected final ArrayList<Map<Message, Message>> aggregateMetrics;

    // protected final Map<Message, Message> aggregateMetrics = new HashMap<>();
    protected final Timer scheduler = new Timer(AGGREGATOR_THREAD_NAME, true);

    protected final int shardGranularity;
    protected final long flushInterval;

    private final StatsDProcessor processor;

    private class FlushTask extends TimerTask {
        @Override
        public void run() {
            for (int i=0 ; i<shardGranularity ; i++) {
                Map<Message, Message> map = aggregateMetrics.get(i);

                synchronized (map) {
                    Iterator<Map.Entry<Message, Message>> iter = map.entrySet().iterator();
                    while (iter.hasNext()) {
                        Message msg = iter.next().getValue();
                        msg.setDone(true);
                        processor.send(msg);

                        iter.remove();
                    }
                }
            }
        }
    }

    public StatsDAggregator(final StatsDProcessor processor, final int shards, final long flushInterval) {
        this.processor = processor;
        this.flushInterval = flushInterval;
        this.shardGranularity = shards;
        this.aggregateMetrics = new ArrayList<>(shards);

        for (int i=0 ; i<this.shardGranularity ; i++) {
            this.aggregateMetrics.add(i, new HashMap<Message, Message>());
        }
    }

    /**
     * Start the aggregator flushing scheduler.
     *
     * */
    public void start() {
        if (flushInterval > 0) {
            scheduler.scheduleAtFixedRate(new FlushTask(), flushInterval, flushInterval);
        }
    }

    /**
     * Stop the aggregator flushing scheduler.
     *
     * */
    public void stop() {
        if (flushInterval > 0) {
            scheduler.cancel();
        }
    }

    public boolean isTypeAggregate(Message.Type type) {
        return aggregateSet.contains(type);
    }

    /**
     * Aggregate a message if possible.
     *
     * @param message the dogstatsd Message we wish to aggregate.
     * @return        a boolean reflecting if the message was aggregated.
     *
     * */
    public boolean aggregateMessage(Message message) {
        if (flushInterval == 0 || !isTypeAggregate(message.getType()) || message.getDone()) {
            return false;
        }


        int hash = message.hashCode();
        int bucket = Math.abs(hash % this.shardGranularity);
        Map<Message, Message> map = aggregateMetrics.get(bucket);

        synchronized (map) {
            // For now let's just put the message in the map
            if (!map.containsKey(message)) {
                map.put(message, message);
            } else {
                Message msg = map.get(message);
                msg.aggregate(message);
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
}
