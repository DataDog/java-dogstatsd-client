package com.timgroup.statsd;

class NonBlockingDirectStatsDClient extends NonBlockingStatsDClient implements DirectStatsDClient {

    public NonBlockingDirectStatsDClient(final NonBlockingStatsDClientBuilder builder) throws StatsDClientException {
        super(builder);
    }

    @Override
    public void recordDistributionValues(String aspect, double[] values, double sampleRate, String... tags) {
        if (values != null && values.length > 0) {
            sendMetric(new DoublesStatsDMessage(aspect, Message.Type.DISTRIBUTION, values, sampleRate, 0, tags));
        }
    }

    @Override
    public void recordDistributionValues(String aspect, long[] values, double sampleRate, String... tags) {
        if (values != null && values.length > 0) {
            sendMetric(new LongsStatsDMessage(aspect, Message.Type.DISTRIBUTION, values, sampleRate, 0, tags));
        }
    }

    @Override
    public void recordSketchWithTimestamp(String aspect, long[] values, double sampleRate, long timestamp, String... tags) {
        if (values != null && values.length > 0) {
            sendMetric(new LongSketchMessage(aspect, values, sampleRate, timestamp, tags));
        }
    }

    abstract class MultiValuedStatsDMessage extends Message {
        private final double sampleRate; // NaN for none
        private final long timestamp; // zero for none
        private int metadataSize = -1; // Cache the size of the metadata, -1 means not calculated yet
        private int offset = 0; // The index of the first value that has not been written

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
        public final boolean writeTo(StringBuilder builder, int capacity, String containerID) {
            int metadataSize = metadataSize(builder, containerID);
            writeHeadMetadata(builder);
            boolean partialWrite = writeValuesTo(builder, capacity - metadataSize);
            writeTailMetadata(builder, containerID);
            return partialWrite;

        }

        private int metadataSize(StringBuilder builder, String containerID) {
            if (metadataSize == -1) {
                final int previousLength = builder.length();
                final int previousEncodedLength = Utf8.encodedLength(builder);
                writeHeadMetadata(builder);
                writeTailMetadata(builder, containerID);
                metadataSize = Utf8.encodedLength(builder) - previousEncodedLength;
                builder.setLength(previousLength);
            }
            return metadataSize;
        }

        private void writeHeadMetadata(StringBuilder builder) {
            builder.append(prefix).append(aspect);
        }

        private void writeTailMetadata(StringBuilder builder, String containerID) {
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

        private boolean writeValuesTo(StringBuilder builder, int remainingCapacity) {
            if (offset >= lengthOfValues()) {
                return false;
            }

            int maxLength = builder.length() + remainingCapacity;

            // Add at least one value
            builder.append(':');
            writeValueTo(builder, offset);
            int previousLength = builder.length();

            // Add remaining values up to the max length
            for (int i = offset + 1; i < lengthOfValues(); i++) {
                builder.append(':');
                writeValueTo(builder, i);
                if (builder.length() > maxLength) {
                    builder.setLength(previousLength);
                    offset = i;
                    return true;
                }
                previousLength = builder.length();
            }
            offset = lengthOfValues();
            return false;
        }

        protected abstract int lengthOfValues();

        protected abstract void writeValueTo(StringBuilder buffer, int index);
    }

    final class LongsStatsDMessage extends MultiValuedStatsDMessage {
        private final long[] values;

        LongsStatsDMessage(String aspect, Message.Type type, long[] values, double sampleRate, long timestamp, String[] tags) {
            super(aspect, type, tags, sampleRate, timestamp);
            this.values = values;
        }

        @Override
        protected int lengthOfValues() {
            return values.length;
        }

        @Override
        protected void writeValueTo(StringBuilder buffer, int index) {
            buffer.append(values[index]);
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
        protected int lengthOfValues() {
            return values.length;
        }

        @Override
        protected void writeValueTo(StringBuilder buffer, int index) {
            buffer.append(values[index]);
        }
    }

    final ProtobufWriter pw = new ProtobufWriter();
    final DirectSketch sk = new DirectSketch();

    final class LongSketchMessage extends Message {
        final long[] values;
        final double sampleRate;
        final long timestamp;

        LongSketchMessage(String aspect, long[] values, double sampleRate, long timestamp, String[] tags) {
            super(aspect, Message.Type.SKETCH, tags);
            this.sampleRate = sampleRate;
            this.values = values;
            this.timestamp = timestamp;

        }

        @Override
        public final boolean canAggregate() {
            return false;
        }

        @Override
        public final void aggregate(Message message) {}

        @Override
        public final boolean writeTo(StringBuilder builder, int capacity, String containerID) {
            sk.build(values, sampleRate);

            pw.clear();
            sk.serialize(pw, timestamp);

            builder
                .append(prefix)
                .append(aspect)
                .append(":");

            pw.flip();
            pw.encodeAscii(builder);

            builder.append("|S");

            if (timestamp != 0) {
                builder.append("|T").append(timestamp);
            }
            if (containerID != null && !containerID.isEmpty()) {
                builder.append("|c:").append(containerID);
            }
            tagString(tags, builder);
            builder.append("\n");

            return false;
        }
    }
}
