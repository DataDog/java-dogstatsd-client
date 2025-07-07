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
    final TagsCardinality tagsCardinality;

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

    protected Message(Message.Type type, TagsCardinality cardinality) {
        this("", type, cardinality, null);
    }

    protected Message(String aspect, Message.Type type, TagsCardinality cardinality, String[] tags) {
        if (cardinality == null) {
            throw new NullPointerException("cardinality");
        }
        this.aspect = aspect == null ? "" : aspect;
        this.type = type;
        this.done = false;
        this.tagsCardinality = cardinality;
        this.tags = tags;
    }

    /**
     * Write this message to the provided {@link StringBuilder}. Will
     * be called from the sender threads.
     *
     * @param builder     StringBuilder the text representation will be written to.
     * @param capacity    The capacity of the send buffer.
     * @return boolean indicating whether the message was partially written to the builder.
     *     If true, the method will be called again with the same arguments to continue writing.
     */
    abstract boolean writeTo(StringBuilder builder, int capacity);

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

    public TagsCardinality getTagsCardinality() {
        return tagsCardinality;
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
        int hash = 0;
        hash = hash * HASH_MULTIPLIER + type.hashCode();
        hash = hash * HASH_MULTIPLIER + aspect.hashCode();
        hash = hash * HASH_MULTIPLIER + tagsCardinality.hashCode();
        hash = hash * HASH_MULTIPLIER + Arrays.hashCode(this.tags);
        return hash;
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
                && (this.getTagsCardinality().equals(msg.getTagsCardinality()))
                && Arrays.equals(this.tags, msg.getTags());
        }

        return false;
    }

    @Override
    public int compareTo(Message message) {
        int cmp = getType().compareTo(message.getType());
        if (cmp != 0) {
            return cmp;
        }
        cmp = getAspect().compareTo(message.getAspect());
        if (cmp != 0) {
            return cmp;
        }
        cmp = getTagsCardinality().compareTo(message.getTagsCardinality());
        if (cmp != 0) {
            return cmp;
        }

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
}
