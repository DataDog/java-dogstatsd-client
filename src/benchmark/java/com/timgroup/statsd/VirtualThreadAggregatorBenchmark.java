package com.timgroup.statsd;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

/**
 * Mirrors {@link AggregatorBenchmark} but routes every call through a virtual-thread-per-task
 * executor, exposing the cost of carrier-thread pinning that occurs when a virtual thread holds
 * a monitor lock (the {@code synchronized} block inside
 * {@link StatsDAggregator#aggregateMessage}).
 *
 * <p>Requires Java 21+. The benchmark fails fast with {@link UnsupportedOperationException}
 * when run on an older JVM.
 *
 * <p>Each JMH worker thread (platform thread) submits one task to the virtual-thread executor
 * and blocks on the returned {@link Future}. With {@code @Threads(N)}, up to N virtual threads
 * compete for the sharded {@code synchronized} maps simultaneously, potentially pinning up to
 * {@code min(N, carrier-pool-parallelism)} carrier threads (the carrier pool is a dedicated
 * JDK-internal {@code ForkJoinPool}, defaulting to {@code Runtime.availableProcessors()} workers,
 * configurable via {@code jdk.virtualThreadScheduler.parallelism}).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Benchmark)
public class VirtualThreadAggregatorBenchmark {

    @Param({"1", "2", "64", "1024"})
    int distinctKeys;

    private StatsDAggregator aggregator;
    private Message[] messages;
    // Shared executor – virtual-thread-per-task, one per benchmark trial.
    private ExecutorService executor;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        aggregator = new StatsDAggregator(new AggregatorBenchmark.NoOpProcessor(), StatsDAggregator.DEFAULT_SHARDS, StatsDAggregator.DEFAULT_FLUSH_INTERVAL);
        executor = newVirtualThreadPerTaskExecutor();
        if (executor == null) {
            throw new UnsupportedOperationException(
                    "Virtual threads are not available; run with Java 21+");
        }
    }

    @Setup(Level.Iteration)
    public void clearMaps() {
        // drain accumulated entries so map size stays bounded
        aggregator.flush();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        aggregator.stop();
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Setup(Level.Trial)
    public void createMessages() {
        messages = new Message[distinctKeys];
        for (int i = 0; i < distinctKeys; i++) {
            messages[i] = new AggregatorBenchmark.BenchMessage(
                    "metric." + i, Message.Type.COUNT, 1L);
        }
    }

    // --- benchmarks at various thread counts (mirrors AggregatorBenchmark) ---

    @Benchmark
    @Threads(1)
    public boolean aggregate_t01(ThreadState ts) throws Exception {
        return submit(ts);
    }

    @Benchmark
    @Threads(2)
    public boolean aggregate_t02(ThreadState ts) throws Exception {
        return submit(ts);
    }

    @Benchmark
    @Threads(4)
    public boolean aggregate_t04(ThreadState ts) throws Exception {
        return submit(ts);
    }

    @Benchmark
    @Threads(8)
    public boolean aggregate_t08(ThreadState ts) throws Exception {
        return submit(ts);
    }

    @Benchmark
    @Threads(16)
    public boolean aggregate_t16(ThreadState ts) throws Exception {
        return submit(ts);
    }

    // --- helpers ---

    private boolean submit(final ThreadState ts) throws Exception {
        final Message msg = messages[ts.next(distinctKeys)];
        Future<Boolean> f = executor.submit(() -> aggregator.aggregateMessage(msg));
        return f.get();
    }

    @State(Scope.Thread)
    public static class ThreadState {
        private int index;

        int next(int bound) {
            int i = index;
            index = (i + 1) % bound;
            return i;
        }
    }

    // Reflective factory so the class compiles on Java 8 source level.
    // Returns null (rather than propagating NoSuchMethodException / IllegalAccessException)
    // so setup() can emit a single, descriptive UnsupportedOperationException instead.
    private static ExecutorService newVirtualThreadPerTaskExecutor() {
        try {
            Method m = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) m.invoke(null);
        } catch (Exception e) {
            return null;
        }
    }
}
