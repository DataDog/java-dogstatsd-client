package com.timgroup.statsd;


public abstract class NumericMessage<T extends Number> extends Message {

    protected Number value;

    protected NumericMessage(Message.Type type) {
        super(type);
    }

    protected NumericMessage(String aspect, Message.Type type, T value, String[] tags) {
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
    public void aggregate(Message message) {
        NumericMessage msg = (NumericMessage)message;
        Number value = msg.getValue();
        switch (msg.getType()) {
            case GAUGE:
                setValue(value);
                break;
            default:
                if (value instanceof Double) {
                    setValue(getValue().doubleValue() + value.doubleValue());
                } else if (value instanceof Integer) {
                    setValue(getValue().intValue() + value.intValue());
                } else if (value instanceof Long) {
                    setValue(getValue().longValue() + value.longValue());
                }
        }

        return;
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
     * Set underlying message value.
     * TODO: handle/throw exceptions
     *
     * @return returns the value for the Message
     */
    public void setValue(Number value) {
        this.value = value;
    }

}

