package com.timgroup.statsd;

interface Message {
    /**
     * Write this message to the provided {@link StringBuilder}. Will
     * only ever be called from the sender thread.
     *
     * @param builder
     */
    void writeTo(StringBuilder builder);
}

