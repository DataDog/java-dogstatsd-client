package com.timgroup.statsd;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Thread)
public class FullPrecisionBenchmark {

    @Param({"3.141592653589793", "3.3E20", "42.0", "0.423", "243.5"})
    public double value;

    private final StringBuilder builder = new StringBuilder(64);

    @Benchmark
    public int format_default() {
        builder.setLength(0);
        builder.append(NonBlockingStatsDClient.format(NonBlockingStatsDClient.NUMBER_FORMATTER, value));
        return builder.length();
    }

    @Benchmark
    public int format_full_precision() {
        builder.setLength(0);
        builder.append(value);
        return builder.length();
    }
}
