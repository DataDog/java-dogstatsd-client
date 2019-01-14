package com.timgroup.statsd;

/**
 * Signals that we've been passed a message that's too long to send
 *
 * @author Taylor Schilling
 */

public class UnsendableMessageException extends RuntimeException {

    private final String unsendableMessage;

    public UnsendableMessageException(final String unsendableMessage) {
        this.unsendableMessage = unsendableMessage;
    }

    public String getUnsendableMessage() {
        return unsendableMessage;
    }
}
