package com.timgroup.statsd;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Taylor Schilling
 */
public class RecordingErrorHandler implements StatsDClientErrorHandler {
    private final List<Exception> exceptions = new ArrayList<Exception>();

    @Override
    public void handle(final Exception exception) {
        exceptions.add(exception);
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }
}
