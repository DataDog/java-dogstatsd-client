package com.timgroup.statsd;

final class NonBlockingDirectStatsDClient extends NonBlockingStatsDClient implements DirectStatsDClient {

    public NonBlockingDirectStatsDClient(final NonBlockingStatsDClientBuilder builder) throws StatsDClientException {
        super(builder);
    }

    @Override
    public void recordDistributionValues(String aspect, double[] values, double sampleRate, String... tags) {
        if ((Double.isNaN(sampleRate) || !isInvalidSample(sampleRate)) && values != null && values.length > 0) {
            sendMetric(new DoublesStatsDMessage(aspect, Message.Type.DISTRIBUTION, values, sampleRate, 0, tags));
        }
    }

    @Override
    public void recordDistributionValues(String aspect, long[] values, double sampleRate, String... tags) {
        if ((Double.isNaN(sampleRate) || !isInvalidSample(sampleRate)) && values != null && values.length > 0) {
            sendMetric(new LongsStatsDMessage(aspect, Message.Type.DISTRIBUTION, values, sampleRate, 0, tags));
        }
    }

    abstract class MultiValuedStatsDMessage extends Message {
        private final double sampleRate; // NaN for none
        private final long timestamp; // zero for none

        MultiValuedStatsDMessage(String aspect, Message.Type type, String[] tags, double sampleRate, long timestamp) {
            super(aspect, type, tags);
            this.sampleRate = sampleRate;
            this.timestamp = timestamp;
        }

        @Override
        public final boolean canAggregate() {
            return false;
        }

        @Override
        public final void aggregate(Message message) {
        }

        @Override
        public final void writeTo(StringBuilder builder, String containerID) {
            builder.append(prefix).append(aspect);
            writeValuesTo(builder);
            builder.append('|').append(type);
            if (!Double.isNaN(sampleRate)) {
                builder.append('|').append('@').append(format(SAMPLE_RATE_FORMATTER, sampleRate));
            }
            if (timestamp != 0) {
                builder.append("|T").append(timestamp);
            }
            tagString(tags, builder);
            if (containerID != null && !containerID.isEmpty()) {
                builder.append("|c:").append(containerID);
            }

            builder.append('\n');
        }

        protected abstract void writeValuesTo(StringBuilder builder);
    }

    final class LongsStatsDMessage extends MultiValuedStatsDMessage {
        private final long[] values;

        LongsStatsDMessage(String aspect, Message.Type type, long[] values, double sampleRate, long timestamp, String[] tags) {
            super(aspect, type, tags, sampleRate, timestamp);
            this.values = values;
        }

        @Override
        protected void writeValuesTo(StringBuilder builder) {
            for (long value : values) {
                builder.append(':').append(value);
            }
        }
    }

    final class DoublesStatsDMessage extends MultiValuedStatsDMessage {
        private final double[] values;

        DoublesStatsDMessage(String aspect, Message.Type type, double[] values, double sampleRate, long timestamp,
                             String[] tags) {
            super(aspect, type, tags, sampleRate, timestamp);
            this.values = values;
        }

        @Override
        protected void writeValuesTo(StringBuilder builder) {
            for (double value : values) {
                builder.append(':').append(value);
            }
        }
    }
}
