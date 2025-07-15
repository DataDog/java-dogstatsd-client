package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MessageTest {
    @Test
    public void testMessageHashcode() throws Exception {

        StatsDTestMessage previous =
                new StatsDTestMessage<Long>(
                        "my.count", Message.Type.COUNT, Long.valueOf(1), 0, new String[0]) {
                    @Override
                    protected void writeValue(StringBuilder builder) {
                        builder.append(this.value);
                    }
                    ;
                };
        StatsDTestMessage previousNewAspectString =
                new StatsDTestMessage<Long>(
                        new String("my.count"),
                        Message.Type.COUNT,
                        Long.valueOf(1),
                        0,
                        new String[0]) {
                    @Override
                    protected void writeValue(StringBuilder builder) {
                        builder.append(this.value);
                    }
                    ;
                };
        StatsDTestMessage previousTagged =
                new StatsDTestMessage<Long>(
                        "my.count",
                        Message.Type.COUNT,
                        Long.valueOf(1),
                        0,
                        new String[] {"foo", "bar"}) {

                    @Override
                    protected void writeValue(StringBuilder builder) {
                        builder.append(this.value);
                    }
                    ;
                };

        StatsDTestMessage next =
                new StatsDTestMessage<Long>(
                        "my.count", Message.Type.COUNT, Long.valueOf(1), 0, new String[0]) {
                    @Override
                    protected void writeValue(StringBuilder builder) {
                        builder.append(this.value);
                    }
                    ;
                };
        StatsDTestMessage nextTagged =
                new StatsDTestMessage<Long>(
                        "my.count",
                        Message.Type.COUNT,
                        Long.valueOf(1),
                        0,
                        new String[] {"foo", "bar"}) {

                    @Override
                    protected void writeValue(StringBuilder builder) {
                        builder.append(this.value);
                    }
                    ;
                };

        assertEquals(previous.hashCode(), next.hashCode());
        assertEquals(previousTagged.hashCode(), nextTagged.hashCode());
        assertEquals(previous.hashCode(), previousNewAspectString.hashCode());
        assertEquals(previous, previousNewAspectString);

        class TestAlphaNumericMessage extends AlphaNumericMessage {
            public TestAlphaNumericMessage(String aspect, Type type, String value, String[] tags) {
                super(aspect, type, value, TagsCardinality.DEFAULT, tags);
            }

            @Override
            boolean writeTo(StringBuilder builder, int capacity) {
                return false;
            }
        }
        AlphaNumericMessage alphaNum1 =
                new TestAlphaNumericMessage(
                        "my.count", Message.Type.COUNT, "value", new String[] {"tag"});
        AlphaNumericMessage alphaNum2 =
                new TestAlphaNumericMessage(
                        new String("my.count"),
                        Message.Type.COUNT,
                        new String("value"),
                        new String[] {new String("tag")});
        assertEquals(alphaNum1, alphaNum2);
    }
}
