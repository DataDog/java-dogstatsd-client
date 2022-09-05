package com.timgroup.statsd;

class StatsDTestMessage<T extends Number> extends NumericMessage<T> {
    final double sampleRate; // NaN for none

    protected StatsDTestMessage(String aspect, Message.Type type, T value, double sampleRate, String[] tags) {
        super(aspect, type, value, tags);
        this.sampleRate = sampleRate;
    }

    @Override
    public final void writeTo(StringBuilder builder, String containerID) {
        builder.append("test.").append(aspect).append(':');
        writeValue(builder);
        builder.append('|').append(type);
        if (!Double.isNaN(sampleRate)) {
            builder.append('|').append('@').append(NonBlockingStatsDClient.format(NonBlockingStatsDClient.SAMPLE_RATE_FORMATTER, sampleRate));
        }
        NonBlockingStatsDClient.tagString(this.tags, "", builder, false);
        if (containerID != null && !containerID.isEmpty()) {
            builder.append("|c:").append(containerID);
        }

        builder.append('\n');
    }

    protected void writeValue(StringBuilder builder) {
        builder.append(NonBlockingStatsDClient.format(NonBlockingStatsDClient.NUMBER_FORMATTER, this.value));
    }
}
