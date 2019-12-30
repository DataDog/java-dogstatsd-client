package com.timgroup.statsd;

/**
 * Describes a handler capable of processing exceptions that occur during StatsD client operations.
 *
 * @author Tom Denley
 *
 */
public interface StatsDClientErrorHandler {

    /**
     * Handle the given exception, which occurred during a StatsD client operation.
     *
     * Should normally be implemented as a synchronized method.
     *
     * @param exception
     *     the {@link Exception} that occurred
     */
    void handle(Exception exception);

}
