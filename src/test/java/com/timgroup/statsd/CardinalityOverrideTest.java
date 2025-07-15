package com.timgroup.statsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasItem;

import java.io.IOException;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CardinalityOverrideTest {
    private static Logger log = Logger.getLogger("NonBlockingStatsDClientTest");

    static final class Case {
        final TagsCardinality value;
        final String expect;

        Case(final TagsCardinality value, final String expect) {
            this.value = value;
            this.expect = expect;
        }
    }

    @Parameters
    public static Object[][] parameters() {
        return TestHelpers.permutations(
                // test permutations of values being set or not or set to default, but when set
                // cardinality value is not
                // inspected, so no need to test permutations of low, orchestrator, high
                new Object[][] {
                    {null, "", "high"},
                    {
                        new Case(null, null),
                        new Case(TagsCardinality.DEFAULT, ""),
                        new Case(TagsCardinality.LOW, "low"),
                    }
                });
    }

    @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    String clientTagsCardinality;
    Case msgTagsCardinality;
    UDPDummyStatsDServer server;
    NonBlockingStatsDClient client;

    public CardinalityOverrideTest(String clientTagsCardinality, Case msgTagsCardinality) {
        log.info(String.format("%s %s", clientTagsCardinality, msgTagsCardinality.value));
        this.clientTagsCardinality = clientTagsCardinality;
        this.msgTagsCardinality = msgTagsCardinality;
    }

    private void assertPayload(final String payloadHead) {
        StringBuilder sb = new StringBuilder(payloadHead);
        if ("".equals(msgTagsCardinality.expect)) {
            // nothing, use agent default
        } else if (msgTagsCardinality.expect != null) {
            sb.append("|card:").append(msgTagsCardinality.expect);
        } else if ("".equals(clientTagsCardinality)) {
            // nothing, use agent default
        } else if (clientTagsCardinality != null) {
            sb.append("|card:").append(clientTagsCardinality);
        }
        server.waitForMessage(payloadHead);
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo(sb.toString())));
    }

    @Before
    public void start() throws IOException {
        if (clientTagsCardinality != null) {
            environmentVariables.set("DD_CARDINALITY", clientTagsCardinality);
        }
        server = new UDPDummyStatsDServer(0);
        client =
                new NonBlockingStatsDClientBuilder()
                        .prefix("my.prefix")
                        .hostname("localhost")
                        .port(server.getPort())
                        .enableTelemetry(false)
                        .originDetectionEnabled(false)
                        .enableAggregation(false)
                        .errorHandler(
                                new StatsDClientErrorHandler() {
                                    public void handle(Exception ex) {
                                        log.info(ex.toString());
                                    }
                                })
                        .build();
    }

    @After
    public void stop() throws IOException {
        client.stop();
        server.close();
    }

    @Test(timeout = 1000)
    public void count_long() {
        client.count("count", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.count:1|c");
    }

    @Test(timeout = 1000)
    public void count_double() {
        client.count("count", 1.0, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.count:1|c");
    }

    @Test(timeout = 1000)
    public void count_long_with_timestamp() {
        client.countWithTimestamp("count", 1, 1000, msgTagsCardinality.value);
        assertPayload("my.prefix.count:1|c|T1000");
    }

    @Test(timeout = 1000)
    public void count_double_with_timestamp() {
        client.countWithTimestamp("count", 1.0, 1000, msgTagsCardinality.value);
        assertPayload("my.prefix.count:1|c|T1000");
    }

    @Test(timeout = 1000)
    public void record_gauge_long() {
        client.recordGaugeValue("gauge", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.gauge:1|g");
    }

    @Test(timeout = 1000)
    public void record_gauge_double() {
        client.recordGaugeValue("gauge", 1.0, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.gauge:1|g");
    }

    @Test(timeout = 1000)
    public void gauge_long() {
        client.gauge("gauge", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.gauge:1|g");
    }

    @Test(timeout = 1000)
    public void gauge_double() {
        client.gauge("gauge", 1.0, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.gauge:1|g");
    }

    @Test(timeout = 1000)
    public void gauge_with_timestamp_double() {
        client.gaugeWithTimestamp("gauge", 1.0, 1000, msgTagsCardinality.value);
        assertPayload("my.prefix.gauge:1|g|T1000");
    }

    @Test(timeout = 1000)
    public void gauge_with_timestamp_long() {
        client.gaugeWithTimestamp("gauge", 1, 1000, msgTagsCardinality.value);
        assertPayload("my.prefix.gauge:1|g|T1000");
    }

    @Test(timeout = 1000)
    public void record_execution_time_long() {
        client.recordExecutionTime("timing", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.timing:1|ms");
    }

    @Test(timeout = 1000)
    public void time() {
        client.recordExecutionTime("timing", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.timing:1|ms");
    }

    @Test(timeout = 1000)
    public void record_histogram_value_double() {
        client.recordHistogramValue("histogram", 1.0, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.histogram:1|h");
    }

    @Test(timeout = 1000)
    public void record_histogram_value_long() {
        client.recordHistogramValue("histogram", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.histogram:1|h");
    }

    @Test(timeout = 1000)
    public void histogram_double() {
        client.histogram("histogram", 1.0, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.histogram:1|h");
    }

    @Test(timeout = 1000)
    public void histogram_long() {
        client.histogram("histogram", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.histogram:1|h");
    }

    @Test(timeout = 1000)
    public void record_distribution_value_double() {
        client.recordDistributionValue("distribution", 1.0, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.distribution:1|d");
    }

    @Test(timeout = 1000)
    public void record_distribution_value_long() {
        client.recordDistributionValue("distribution", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.distribution:1|d");
    }

    @Test(timeout = 1000)
    public void distribution_double() {
        client.distribution("distribution", 1.0, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.distribution:1|d");
    }

    @Test(timeout = 1000)
    public void distribution_long() {
        client.distribution("distribution", 1, Double.NaN, msgTagsCardinality.value);
        assertPayload("my.prefix.distribution:1|d");
    }

    @Test(timeout = 1000)
    public void record_event() {
        client.recordEvent(
                Event.builder()
                        .withTitle("foo")
                        .withTagsCardinality(msgTagsCardinality.value)
                        .build());
        assertPayload("_e{13,0}:my.prefix.foo|");
    }

    @Test(timeout = 1000)
    public void record_service_check_run() {
        client.recordServiceCheckRun(
                ServiceCheck.builder()
                        .withName("foo")
                        .withStatus(ServiceCheck.Status.OK)
                        .withTagsCardinality(msgTagsCardinality.value)
                        .build());
        assertPayload("_sc|foo|0");
    }
}
