package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.util.Arrays;
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

    protected final String AGGREGATOR_THREAD_NAME = "statsd-aggregator-thread";
    protected final Set<Message.Type> aggregateSet = new HashSet<>(
            Arrays.asList(Message.Type.COUNT, Message.Type.GAUGE));
    protected final Map<Message, Message> aggregateMetrics = new HashMap<>();
    protected final Timer scheduler = new Timer(AGGREGATOR_THREAD_NAME, true);
    protected final long flushInterval;

    private final StatsDProcessor processor;

    private class FlushTask extends TimerTask {
        @Override
        public void run() {
            synchronized (aggregateMetrics) {
                Iterator<Map.Entry<Message, Message>> iter = aggregateMetrics.entrySet().iterator();
                while (iter.hasNext()) {
                    Message msg = iter.next().getValue();
                    msg.setDone(true);
                    processor.send(msg);

                    iter.remove();
                }
            }
        }
    }

    public StatsDAggregator(final StatsDProcessor processor, final long flushInterval) {
        this.processor = processor;
        this.flushInterval = flushInterval;
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

        synchronized (aggregateMetrics) {
            // For now let's just put the message in the map
            if (!aggregateMetrics.containsKey(message)) {
                aggregateMetrics.put(message, message);
            } else {
                Message msg = aggregateMetrics.get(message);
                msg.aggregate(message);
            }
        }

        return true;
    }

    public final long getFlushInterval() {
        return this.flushInterval;
    }
}
