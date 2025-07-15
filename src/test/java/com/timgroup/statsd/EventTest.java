package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import org.junit.Test;

public class EventTest {
    @Test
    public void builds() {
        final Event event =
                Event.builder()
                        .withTitle("title1")
                        .withText("text1")
                        .withDate(1234)
                        .withHostname("host1")
                        .withPriority(Event.Priority.LOW)
                        .withAggregationKey("key1")
                        .withAlertType(Event.AlertType.ERROR)
                        .withSourceTypeName("sourceType1")
                        .build();

        assertEquals("title1", event.getTitle());
        assertEquals("text1", event.getText());
        assertEquals(1234, event.getMillisSinceEpoch());
        assertEquals("host1", event.getHostname());
        assertEquals("low", event.getPriority());
        assertEquals("key1", event.getAggregationKey());
        assertEquals("error", event.getAlertType());
        assertEquals("sourceType1", event.getSourceTypeName());
    }

    @Test
    public void builds_with_defaults() {
        final Event event = Event.builder().withTitle("title1").withText("text1").build();

        assertEquals("title1", event.getTitle());
        assertEquals("text1", event.getText());
        assertEquals(-1, event.getMillisSinceEpoch());
        assertEquals(null, event.getHostname());
        assertEquals(null, event.getPriority());
        assertEquals(null, event.getAggregationKey());
        assertEquals(null, event.getAlertType());
        assertEquals(null, event.getSourceTypeName());
    }

    @Test(expected = IllegalStateException.class)
    public void fails_without_title() {
        Event.builder()
                .withText("text1")
                .withDate(1234)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourceType1")
                .build();
    }

    @Test
    public void builds_with_date() {
        final long expectedMillis = 1234567000;
        final Date date = new Date(expectedMillis);
        final Event event =
                Event.builder().withTitle("title1").withText("text1").withDate(date).build();

        assertEquals("title1", event.getTitle());
        assertEquals("text1", event.getText());
        assertEquals(expectedMillis, event.getMillisSinceEpoch());
    }

    @Test
    public void builds_without_text() {
        final long expectedMillis = 1234567000;
        final Date date = new Date(expectedMillis);
        final Event event = Event.builder().withTitle("title1").withDate(date).build();

        assertEquals("title1", event.getTitle());
        assertEquals(null, event.getText());
        assertEquals(expectedMillis, event.getMillisSinceEpoch());
    }
}
