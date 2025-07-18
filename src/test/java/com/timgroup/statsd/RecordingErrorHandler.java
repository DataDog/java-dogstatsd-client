package com.timgroup.statsd;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Taylor Schilling
 */
public class RecordingErrorHandler implements StatsDClientErrorHandler {
    private final Queue<Exception> exceptions = new ConcurrentLinkedQueue<>();

    @Override
    public void handle(final Exception exception) {
        exceptions.add(exception);
    }

    public List<Exception> getExceptions() {
        return new ArrayList(exceptions);
    }
}
