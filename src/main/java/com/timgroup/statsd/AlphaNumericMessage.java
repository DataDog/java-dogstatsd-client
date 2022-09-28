package com.timgroup.statsd;

public abstract class AlphaNumericMessage extends Message {

    protected final String value;

    protected AlphaNumericMessage(Message.Type type, String value) {
        super(type);
        this.value = value;
    }

    protected AlphaNumericMessage(String aspect, Message.Type type, String value, String[] tags) {
        super(aspect, type, tags);
        this.value = value;
    }

    /**
     * Aggregate message.
     *
     * @param message
     *     Message to aggregate.
     */
    @Override
    public void aggregate(Message message) { }

    /**
     * Get underlying message value.
     *
     * @return returns the value for the Message
     */
    public String getValue() {
        return this.value;
    }

    @Override
    public int hashCode() {

        // cache it
        if (this.hash == Integer.MIN_VALUE) {
            this.hash = super.hashCode() * HASH_MULTIPLIER + this.value.hashCode();
        }

        return this.hash;
    }

    @Override
    public boolean equals(Object object) {
        boolean equal = super.equals(object);
        if (!equal) {
            return false;
        }

        if (object instanceof AlphaNumericMessage ) {
            AlphaNumericMessage msg = (AlphaNumericMessage)object;
            return this.value.equals(msg.getValue());
        }

        return false;
    }

    @Override
    public int compareTo(Message message) {
        int comparison = super.compareTo(message);
        if (comparison == 0 && message instanceof AlphaNumericMessage) {
            return value.compareTo(((AlphaNumericMessage) message).getValue());
        }
        return comparison;
    }
}

