package com.timgroup.statsd;

import java.util.Date;

/**
 * An event to send.
 * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events">http://docs.datadoghq.com/guides/dogstatsd/#events</a>
 */
public class Event {
    private String title;
    private String text;
    private long millisSinceEpoch = -1;
    private String hostname;
    private String aggregationKey;
    private String priority;
    private String sourceTypeName;
    private String alertType;

    public String getTitle() {
        return title;
    }

    public String getText() {
        return text;
    }

    /**
     * Get number of milliseconds since epoch started.
     * @return -1 if not set
     */
    public long getMillisSinceEpoch() {
        return millisSinceEpoch;
    }

    public String getHostname() {
        return hostname;
    }

    public String getAggregationKey() {
        return aggregationKey;
    }

    public String getPriority() {
        return priority;
    }

    public String getSourceTypeName() {
        return sourceTypeName;
    }

    public String getAlertType() {
        return alertType;
    }

    public static Builder builder() {
        return new Builder();
    }

    private Event(){}

    public enum Priority {
        LOW, NORMAL
    }

    public enum AlertType {
        ERROR, WARNING, INFO, SUCCESS
    }

    @SuppressWarnings({"AccessingNonPublicFieldOfAnotherObject",
            "PrivateMemberAccessBetweenOuterAndInnerClass", "ParameterHidesMemberVariable"})
    public static class Builder {
        private final Event event = new Event();

        private Builder() {}

        /**
         * Build factory method for the event.
         */
        public Event build() {
            if ((event.title == null) || event.title.isEmpty()) {
                throw new IllegalStateException("event title must be set");
            }
            if ((event.text == null) || event.text.isEmpty()) {
                throw new IllegalStateException("event text must be set");
            }
            return event;
        }

        /**
         * Title for the event.
         * @param title
         *     Event title ; mandatory
         */
        public Builder withTitle(final String title) {
            event.title = title;
            return this;
        }

        /**
         * Text for the event.
         * @param text
         *     Event text ; supports line breaks ; mandatory
         */
        public Builder withText(final String text) {
            event.text = text;
            return this;
        }

        /**
         * Date for the event.
         * @param date
         *     Assign a timestamp to the event ; Default: none (Default is the current Unix epoch timestamp when not sent)
         */
        public Builder withDate(final Date date) {
            event.millisSinceEpoch = date.getTime();
            return this;
        }

        /**
         * Date for the event.
         * @param millisSinceEpoch
         *     Assign a timestamp to the event ; Default: none (Default is the current Unix epoch timestamp when not sent)
         */
        public Builder withDate(final long millisSinceEpoch) {
            event.millisSinceEpoch = millisSinceEpoch;
            return this;
        }

        /**
         * Source hostname for the event.
         * @param hostname
         *     Assign a hostname to the event ; Default: none
         */
        public Builder withHostname(final String hostname) {
            event.hostname = hostname;
            return this;
        }

        /**
         * Aggregation key for the event.
         * @param aggregationKey
         *     Assign an aggregation key to the event, to group it with some others ; Default: none
         */
        public Builder withAggregationKey(final String aggregationKey) {
            event.aggregationKey = aggregationKey;
            return this;
        }

        /**
         * Priority for the event.
         * @param priority
         *     Can be "normal" or "low" ; Default: "normal"
         */
        public Builder withPriority(final Priority priority) {
            //noinspection StringToUpperCaseOrToLowerCaseWithoutLocale
            event.priority = priority.name().toLowerCase();
            return this;
        }

        /**
         * Source Type name for the event.
         * @param sourceTypeName
         *     Assign a source type to the event ; Default: none
         */
        public Builder withSourceTypeName(final String sourceTypeName) {
            event.sourceTypeName = sourceTypeName;
            return this;
        }

        /**
         * Alert type for the event.
         * @param alertType
         *     Can be "error", "warning", "info" or "success" ; Default: "info"
         */
        public Builder withAlertType(final AlertType alertType) {
            //noinspection StringToUpperCaseOrToLowerCaseWithoutLocale
            event.alertType = alertType.name().toLowerCase();
            return this;
        }
    }
}
