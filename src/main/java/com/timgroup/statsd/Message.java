package com.timgroup.statsd;

import java.util.Arrays;
import java.util.Objects;

public abstract class Message {
    final String aspect;
    final Message.Type type;
    final String[] tags;
    protected boolean done;
    protected Integer hash;

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
        this.tags = null;
    }

    protected Message(String aspect, Message.Type type, String[] tags) {
        this.aspect = aspect;
        this.type = type;
        this.done = false;
        this.tags = tags;
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
     * Aggregate message.
     *
     * @param message
     *     Message to aggregate.
     */
    public abstract void aggregate(Message message);


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
     * Return the array of tags for the message.
     *
     * @return returns the string array of tags for the Message
     */
    public String[] getTags() {
        return this.tags;
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
        if (this.hash == null) {
            this.hash = new Integer(Objects.hash(this.aspect));
            this.hash += Objects.hash(this.tags);
        }

        return this.hash.intValue();
    }

    /**
     * Messages must implement hashCode.
     */
    @Override
    public boolean equals(Object object) {
        if (object == null) {
            return false;
        }
        if (object instanceof Message) {
            final Message msg = (Message)object;

            boolean equals = (this.getAspect() == msg.getAspect())
                && (this.getType() == msg.getType())
                && (this.done == msg.getDone())
                && Arrays.equals(this.tags, (msg.getTags()));

            return equals;
        }

        return false;
    }
}
