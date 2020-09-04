package com.timgroup.statsd;

import java.util.Objects;


public abstract class AlphaNumericMessage extends Message {

    protected String value;

    protected AlphaNumericMessage(Message.Type type) {
        super(type);
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
     * TODO: handle/throw exceptions
     *
     * @return returns the value for the Message
     */
    public String getValue() {
        return this.value;
    }

    @Override
    public int hashCode() {

        // cache it
        if (this.hash == null) {
            super.hashCode();  // will instantiate hash if null
            if (this.value != null) {
                this.hash += Objects.hash(this.value);
            }
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
            return super.equals(msg) && (this.value == msg.getValue());
        }

        return false;
    }
}

