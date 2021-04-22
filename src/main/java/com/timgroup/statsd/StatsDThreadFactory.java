package com.timgroup.statsd;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

final class StatsDThreadFactory implements ThreadFactory {
    private final ThreadFactory delegate = Executors.defaultThreadFactory();

    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread result = delegate.newThread(runnable);
        result.setName("StatsD-" + result.getName());
        result.setDaemon(true);
        return result;
    }
}
