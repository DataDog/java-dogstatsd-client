package com.timgroup.statsd;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Benchmark)
public class AggregatorBenchmark {

    @Param({"64", "1024"})
    int distinctKeys;

    private StatsDAggregator aggregator;
    private Message[] messages;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        aggregator = new StatsDAggregator(new NoOpProcessor(), 4, 60_000);
    }

    @Setup(Level.Iteration)
    public void clearMaps() {
        // drain accumulated entries so map size stays bounded
        aggregator.flush();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        aggregator.stop();
    }

    @Setup(Level.Trial)
    public void createMessages() {
        messages = new Message[distinctKeys];
        for (int i = 0; i < distinctKeys; i++) {
            messages[i] = new BenchMessage("metric." + i, Message.Type.COUNT, 1L);
        }
    }

    // --- benchmarks at various thread counts ---

    @Benchmark
    @Threads(1)
    public boolean aggregate_t01(ThreadState ts) {
        return aggregator.aggregateMessage(messages[ts.next(distinctKeys)]);
    }

    @Benchmark
    @Threads(2)
    public boolean aggregate_t02(ThreadState ts) {
        return aggregator.aggregateMessage(messages[ts.next(distinctKeys)]);
    }

    @Benchmark
    @Threads(4)
    public boolean aggregate_t04(ThreadState ts) {
        return aggregator.aggregateMessage(messages[ts.next(distinctKeys)]);
    }

    @Benchmark
    @Threads(8)
    public boolean aggregate_t08(ThreadState ts) {
        return aggregator.aggregateMessage(messages[ts.next(distinctKeys)]);
    }

    @Benchmark
    @Threads(16)
    public boolean aggregate_t16(ThreadState ts) {
        return aggregator.aggregateMessage(messages[ts.next(distinctKeys)]);
    }

    // --- per-thread state for round-robin key selection ---

    @State(Scope.Thread)
    public static class ThreadState {
        private int index;

        int next(int bound) {
            int i = index;
            index = (i + 1) % bound;
            return i;
        }
    }

    // --- minimal helpers ---

    static class BenchMessage extends NumericMessage<Long> {
        BenchMessage(String aspect, Message.Type type, Long value) {
            super(aspect, type, value, TagsCardinality.DEFAULT, null);
        }

        @Override
        protected boolean writeTo(StringBuilder builder, int capacity) {
            return false;
        }
    }

    static class NoOpProcessor extends StatsDProcessor {
        NoOpProcessor() throws Exception {
            super(0, new StatsDClientErrorHandler() {
                @Override
                public void handle(Exception ex) {}
            }, 0, 1, 1, 0, 0, new StatsDThreadFactory());
        }

        @Override
        protected ProcessingTask createProcessingTask() {
            return null;
        }

        @Override
        public boolean send(Message message) {
            return true;
        }
    }
}
