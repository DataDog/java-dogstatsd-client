package com.timgroup.statsd;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

public abstract class Message implements Comparable<Message> {

    final String aspect;
    final Message.Type type;
    final String[] tags;
    protected boolean done;

    // borrowed from Array.hashCode implementation:
    // https://github.com/openjdk/jdk11/blob/master/src/java.base/share/classes/java/util/Arrays.java#L4454-L4465
    protected static final int HASH_MULTIPLIER = 31;

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

    protected static final Set<Type> AGGREGATE_SET = EnumSet.of(Type.COUNT, Type.GAUGE, Type.SET);

    protected Message(Message.Type type) {
        this("", type, null);
    }

    protected Message(String aspect, Message.Type type, String[] tags) {
        this.aspect = aspect == null ? "" : aspect;
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
    abstract void writeTo(StringBuilder builder, String containerID);

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
     *
     * @return boolean on whether or not this message type may be aggregated.
     */
    public boolean canAggregate() {
        return AGGREGATE_SET.contains(type);
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
        return type.hashCode() * HASH_MULTIPLIER * HASH_MULTIPLIER
                + aspect.hashCode() * HASH_MULTIPLIER
                + Arrays.hashCode(this.tags);
    }

    /**
     * Messages must implement hashCode.
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof Message) {
            final Message msg = (Message)object;

            return (Objects.equals(this.getAspect(), msg.getAspect()))
                && (this.getType() == msg.getType())
                && Arrays.equals(this.tags, msg.getTags());
        }

        return false;
    }
    
    @Override
    public int compareTo(Message message) {
        int typeComparison = getType().compareTo(message.getType());
        if (typeComparison == 0) {
            int aspectComparison = getAspect().compareTo(message.getAspect());
            if (aspectComparison == 0) {
                if (tags == null && message.tags == null) {
                    return 0;
                } else if (tags == null) {
                    return 1;
                } else if (message.tags == null) {
                    return -1;
                }
                if (tags.length == message.tags.length) {
                    int comparison = 0;
                    for (int i = 0; i < tags.length && comparison == 0; i++) {
                        comparison = tags[i].compareTo(message.tags[i]);
                    }
                    return comparison;
                }
                return tags.length < message.tags.length ? 1 : -1;
            }
            return aspectComparison;
        }
        return typeComparison;
    }
}
