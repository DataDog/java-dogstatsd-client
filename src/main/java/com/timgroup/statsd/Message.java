package com.timgroup.statsd;

interface Message {
    /**
     * Write this message to the provided {@link StringBuilder}. Will
     * be called from the sender threads.
     *
     * @param builder
     *     StringBuilder the text representation will be written to.
     */
    void writeTo(StringBuilder builder);
}

