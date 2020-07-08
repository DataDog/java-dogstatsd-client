package com.timgroup.statsd;

import java.util.Objects;

public abstract class Message<T extends Number> {
    final String aspect;
    final Message.Type type;
    protected Number value;
    protected boolean done;
    protected int hash;

    public enum Type {
        GAUGE("g"),
        COUNT("c"),
        TIME("ms"),
        SET("s"),
        HISTOGRAM("h"),
        DISTRIBUTION("d"),
        EVENT("_e"),
        SERVICE_CHECK("_sc");

        private final String type;

        Type(String type) {
            this.type = type;
        }

        public String toString() {
            return this.type;
        }
    }

    protected Message(Message.Type type) {
        this.aspect = "";
        this.type = type;
        this.done = false;
    }

    protected Message(String aspect, Message.Type type, T value) {
        this.aspect = aspect;
        this.type = type;
        this.value = value;
        this.done = false;
    }

    /**
     * Write this message to the provided {@link StringBuilder}. Will
     * be called from the sender threads.
     *
     * @param builder
     *     StringBuilder the text representation will be written to.
     */
    abstract void writeTo(StringBuilder builder);

    /**
     * Return the message aspect.
     *
     * @return returns the string representing the Message aspect
     */
    public final String getAspect() {
        return this.aspect;
    }

    /**
     * Return the message type.
     *
     * @return returns the dogstatsd type for the Message
     */
    public final Type getType() {
        return this.type;
    }

    /**
     * Get underlying message value.
     * TODO: handle/throw exceptions
     *
     * @return returns the value for the Message
     */
    public Number getValue() {
        return this.value;
    }

    /**
     * Return whether a message can be aggregated.
     * Not sure if this makes sense.
     *
     * @return boolean on whether or not this message type may be aggregated.
     */
    public boolean canAggregate() {
        // return (this.type == m.type);
        return false;
    }

    /**
     * Aggregate message.
     *
     * @param message
     *     Message to aggregate.
     */
    public void aggregate(Message message) {
        Number value = message.getValue();
        switch(message.getType()) {
            case GAUGE:
                this.value = value;
                break;
            default:
                if (value instanceof Double) {
                    this.value = getValue().doubleValue() + value.doubleValue();
                } else if (value instanceof Integer) {
                    this.value = getValue().intValue() + value.intValue();
                } else if (value instanceof Long) {
                    this.value = getValue().longValue() + value.longValue();
                }
        }

        return;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public boolean getDone() {
        return this.done;
    }

    /**
     * Messages must implement hashCode.
     */
    @Override
    public int hashCode() {
        // cache it
        if (this.hash == 0) {
            this.hash = Objects.hash(this.aspect);
        }

        return this.hash;
    }

    /**
     * Messages must implement hashCode.
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof Message) {
            final Message msg = (Message)o;

            boolean equals = (this.getAspect() == msg.getAspect())
                && (this.getType() == msg.getType())
                && (this.done == msg.getDone());

            return equals;
        }

        return false;
    }
}

