package com.timgroup.statsd;

/**
 * Signals that we've been passed a message that's invalid and won't be sent
 *
 * @author Taylor Schilling
 */

public class InvalidMessageException extends RuntimeException {

    private final String invalidMessage;

    /**
     * Creates an InvalidMessageException with a specified detail message and the invalid message itself
     *
     * @param detailMessage a message that details why the invalid message is considered so
     * @param invalidMessage the message deemed invalid
     */
    public InvalidMessageException(final String detailMessage, final String invalidMessage) {
        super(detailMessage);
        this.invalidMessage = invalidMessage;
    }

    public String getInvalidMessage() {
        return invalidMessage;
    }
}
