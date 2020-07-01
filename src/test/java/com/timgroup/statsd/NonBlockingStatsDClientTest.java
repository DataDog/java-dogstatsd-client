package com.timgroup.statsd;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NonBlockingStatsDClientTest {

    private static final int STATSD_SERVER_PORT = 17254;
    private static final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
        .prefix("my.prefix")
        .hostname("localhost")
        .port(STATSD_SERVER_PORT)
        .enableTelemetry(false)
        .build();
    private static DummyStatsDServer server;

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientTest");

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @BeforeClass
    public static void start() throws IOException {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
    }

    @AfterClass
    public static void stop() {
        try {
            client.stop();
            server.close();
        } catch (java.io.IOException ignored) {
        }
    }

    @After
    public void clear() {
        server.clear();
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd() throws Exception {

        client.count("mycount", 24);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_with_sample_rate_to_statsd() throws Exception {

        client.count("mycount", 24, 1);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|@1.000000")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd_with_null_tags() throws Exception {

        client.count("mycount", 24, (java.lang.String[]) null);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd_with_empty_tags() throws Exception {

        client.count("mycount", 24);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd_with_tags() throws Exception {

        client.count("mycount", 24, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_with_sample_rate_to_statsd_with_tags() throws Exception {

        client.count("mycount", 24, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|@1.000000|#baz,foo:bar")));
    }


    @Test(timeout = 5000L)
    public void sends_counter_increment_to_statsd() throws Exception {

        client.incrementCounter("myinc");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myinc:1|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_increment_to_statsd_with_tags() {

        client.incrementCounter("myinc", "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myinc:1|c|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_increment_with_sample_rate_to_statsd_with_tags() throws Exception {

        client.incrementCounter("myinc", 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myinc:1|c|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_decrement_to_statsd() throws Exception {

        client.decrementCounter("mydec");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydec:-1|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_decrement_to_statsd_with_tags() throws Exception {

        client.decrementCounter("mydec", "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydec:-1|c|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_decrement_with_sample_rate_to_statsd_with_tags() throws Exception {


        client.decrementCounter("mydec", 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydec:-1|c|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_with_sample_rate_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 423, 1);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|@1.000000")));
    }

    @Test(timeout = 5000L)
    public void sends_large_double_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 123456789012345.67890);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:123456789012345.67|g")));
    }

    @Test(timeout = 5000L)
    public void sends_exact_double_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 123.45678901234567890);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:123.456789|g")));
    }

    @Test(timeout = 5000L)
    public void sends_double_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 0.423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:0.423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_to_statsd_with_tags() throws Exception {


        client.recordGaugeValue("mygauge", 423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_with_sample_rate_to_statsd_with_tags() throws Exception {


        client.recordGaugeValue("mygauge", 423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_gauge_to_statsd_with_tags() throws Exception {


        client.recordGaugeValue("mygauge", 0.423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:0.423|g|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_histogram_to_statsd() throws Exception {

        client.recordHistogramValue("myhistogram", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:423|h")));
    }

    @Test(timeout = 5000L)
    public void sends_double_histogram_to_statsd() throws Exception {


        client.recordHistogramValue("myhistogram", 0.423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:0.423|h")));
    }

    @Test(timeout = 5000L)
    public void sends_histogram_to_statsd_with_tags() throws Exception {


        client.recordHistogramValue("myhistogram", 423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:423|h|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_histogram_with_sample_rate_to_statsd_with_tags() throws Exception {


        client.recordHistogramValue("myhistogram", 423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:423|h|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_histogram_to_statsd_with_tags() throws Exception {


        client.recordHistogramValue("myhistogram", 0.423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:0.423|h|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_histogram_with_sample_rate_to_statsd_with_tags() throws Exception {


        client.recordHistogramValue("myhistogram", 0.423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:0.423|h|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_distribtuion_to_statsd() throws Exception {

        client.recordDistributionValue("mydistribution", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423|d")));
    }

    @Test(timeout = 5000L)
    public void sends_double_distribution_to_statsd() throws Exception {


        client.recordDistributionValue("mydistribution", 0.423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:0.423|d")));
    }

    @Test(timeout = 5000L)
    public void sends_distribution_to_statsd_with_tags() throws Exception {


        client.recordDistributionValue("mydistribution", 423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423|d|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_distribution_with_sample_rate_to_statsd_with_tags() throws Exception {


        client.recordDistributionValue("mydistribution", 423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423|d|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_distribution_to_statsd_with_tags() throws Exception {


        client.recordDistributionValue("mydistribution", 0.423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:0.423|d|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_distribution_with_sample_rate_to_statsd_with_tags() throws Exception {


        client.recordDistributionValue("mydistribution", 0.423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:0.423|d|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_timer_to_statsd() throws Exception {


        client.recordExecutionTime("mytime", 123);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms")));
    }

    /**
     * A regression test for <a href="https://github.com/indeedeng/java-dogstatsd-client/issues/3">this i18n number formatting bug</a>
     *
     * @throws Exception
     */
    @Test
    public void
    sends_timer_to_statsd_from_locale_with_unamerican_number_formatting() throws Exception {

        Locale originalDefaultLocale = Locale.getDefault();

        // change the default Locale to one that uses something other than a '.' as the decimal separator (Germany uses a comma)
        Locale.setDefault(Locale.GERMANY);

        try {

            client.recordExecutionTime("mytime", 123, "foo:bar", "baz");
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms|#baz,foo:bar")));
        } finally {
            // reset the default Locale in case changing it has side-effects
            Locale.setDefault(originalDefaultLocale);
        }
    }


    @Test(timeout = 5000L)
    public void sends_timer_to_statsd_with_tags() throws Exception {


        client.recordExecutionTime("mytime", 123, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_timer_with_sample_rate_to_statsd_with_tags() throws Exception {


        client.recordExecutionTime("mytime", 123, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT, Integer.MAX_VALUE, "instance:foo", "app:bar");
        client.gauge("value", 423, "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo,baz")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .build();
        client.gauge("value", 423, "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo,baz")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags_with_sample_rate_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT, Integer.MAX_VALUE, "instance:foo", "app:bar");
        client.gauge("value", 423, 1, "baz");
        server.waitForMessage("my.prefix.value:423");

        List<String> messages = server.messagesReceived();

        assertThat(messages, hasItem(comparesEqualTo("my.prefix.value:423|g|@1.000000|#app:bar,instance:foo,baz")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags_with_sample_rate() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .build();
        client.gauge("value", 423, 1, "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|@1.000000|#app:bar,instance:foo,baz")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_constant_tags_only_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT, Integer.MAX_VALUE, "instance:foo", "app:bar");
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_constant_tags_only() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .build();
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env_deprecated() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT, Integer.MAX_VALUE);
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .build();
        client.gauge("value", 423);
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env_and_constant_tags_deprecated() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final String constantTags = "arbitraryTag:arbitraryValue";
        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT, constantTags);
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity," + constantTags)));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env_and_constant_tags() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final String constantTags = "arbitraryTag:arbitraryValue";
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .constantTags(constantTags)
            .build();
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity," + constantTags)));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_args_deprecated() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT, Integer.MAX_VALUE, null, null, entity_value+"-arg");
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity-arg")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_args() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .entityID(entity_value+"-arg")
            .build();
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity-arg")));
    }


    @Test(timeout = 5000L)
    public void init_client_from_env_vars() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_DOGSTATSD_PORT_ENV_VAR, Integer.toString(STATSD_SERVER_PORT));
        environmentVariables.set(NonBlockingStatsDClient.DD_AGENT_HOST_ENV_VAR, "localhost");
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .build();
        client.gauge("value", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g")));
    }

    @Test(timeout = 5000L)
    public void checkEnvVars() {
        final Random r = new Random();
        for (final NonBlockingStatsDClient.Literal literal : NonBlockingStatsDClient.Literal.values()) {
            final String envVarName = literal.envName();
            final String randomString = envVarName + "_val_" +r.nextDouble();
            environmentVariables.set(envVarName, randomString);
            final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
                    .prefix("checkEnvVars")
                    .hostname("localhost")
                    .port(STATSD_SERVER_PORT)
                    .build();
            server.clear();
            client.gauge("value", 42);
            server.waitForMessage("checkEnvVars.value");
            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("checkEnvVars.value:42|g|#" +
                    literal.tag() + ":" + randomString)));
            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("checkEnvVars.value:42|g|#" +
                    envVarName.replace("DD_", "").toLowerCase() + ":" + randomString)));
            server.clear();

            environmentVariables.clear(envVarName);
            log.info("passed for '" + literal + "'; env cleaned.");
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_empty_prefix_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("", "localhost", STATSD_SERVER_PORT);
        client.gauge("top.level.value", 423);
        server.waitForMessage("top.level");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_empty_prefix() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .build();
        client.gauge("top.level.value", 423);
        server.waitForMessage("top.level");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_null_prefix_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClient(null, "localhost", STATSD_SERVER_PORT);
        client.gauge("top.level.value", 423);
        server.waitForMessage("top.level");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_null_prefix() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix(null)
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .build();
        client.gauge("top.level.value", 423);
        server.waitForMessage("top.level");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_no_prefix() throws Exception {

        final NonBlockingStatsDClient no_prefix_client = new NonBlockingStatsDClientBuilder()
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .build();
        no_prefix_client.gauge("top.level.value", 423);
        server.waitForMessage("top.level");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_event() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1\nline2")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        client.recordEvent(event);
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{16,12}:my.prefix.title1|text1\\nline2|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1")));
    }

    @Test(timeout = 5000L)
    public void sends_partial_event() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .build();
        client.recordEvent(event);
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{16,5}:my.prefix.title1|text1|d:1234567")));
    }

    @Test(timeout = 5000L)
    public void sends_event_with_tags() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        client.recordEvent(event, "foo:bar", "baz");
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{16,5}:my.prefix.title1|text1|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_partial_event_with_tags() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .build();
        client.recordEvent(event, "foo:bar", "baz");
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{16,5}:my.prefix.title1|text1|d:1234567|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_event_empty_prefix_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClient("", "localhost", STATSD_SERVER_PORT);
        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        client.recordEvent(event, "foo:bar", "baz");
        server.waitForMessage("_e");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{6,5}:title1|text1|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_event_empty_prefix() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .build();

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        client.recordEvent(event, "foo:bar", "baz");
        server.waitForMessage("_e");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{6,5}:title1|text1|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_service_check() throws Exception {
        final String inputMessage = "\u266c \u2020\u00f8U \n\u2020\u00f8U \u00a5\u00bau|m: T0\u00b5 \u266a"; // "♬ †øU \n†øU ¥ºu|m: T0µ ♪"
        final String outputMessage = "\u266c \u2020\u00f8U \\n\u2020\u00f8U \u00a5\u00bau|m\\: T0\u00b5 \u266a"; // note the escaped colon
        final String[] tags = {"key1:val1", "key2:val2"};
        final ServiceCheck sc = ServiceCheck.builder()
                .withName("my_check.name")
                .withStatus(ServiceCheck.Status.WARNING)
                .withMessage(inputMessage)
                .withHostname("i-abcd1234")
                .withTags(tags)
                .withTimestamp(1420740000)
                .build();

        assertEquals(outputMessage, sc.getEscapedMessage());

        client.serviceCheck(sc);
        server.waitForMessage("_sc");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo(String.format("_sc|my_check.name|1|d:1420740000|h:i-abcd1234|#key2:val2,key1:val1|m:%s",
                outputMessage))));
    }

    @Test(timeout = 5000L)
    public void sends_nan_gauge_to_statsd() throws Exception {
        client.recordGaugeValue("mygauge", Double.NaN);

        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:NaN|g")));
    }

    @Test(timeout = 5000L)
    public void sends_set_to_statsd() throws Exception {
        client.recordSetValue("myset", "myuserid");

        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myset:myuserid|s")));

    }

    @Test(timeout = 5000L)
    public void sends_set_to_statsd_with_tags() throws Exception {
        client.recordSetValue("myset", "myuserid", "foo:bar", "baz");

        server.waitForMessage("my.prefix.myset");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myset:myuserid|s|#baz,foo:bar")));

    }

    @Test(timeout=5000L)
    public void sends_too_large_message_deprecated() throws Exception {

        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();

        try (final NonBlockingStatsDClient testClient = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT,  null, errorHandler)) {

            final byte[] messageBytes = new byte[1600];
            final ServiceCheck tooLongServiceCheck = ServiceCheck.builder()
                    .withName("toolong")
                    .withMessage(new String(messageBytes))
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(tooLongServiceCheck);

            final ServiceCheck withinLimitServiceCheck = ServiceCheck.builder()
                    .withName("fine")
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(withinLimitServiceCheck);

            server.waitForMessage("_sc");

            final List<Exception> exceptions = errorHandler.getExceptions();
            assertEquals(1, exceptions.size());
            final Exception exception = exceptions.get(0);
            assertEquals(InvalidMessageException.class, exception.getClass());
            assertTrue(((InvalidMessageException)exception).getInvalidMessage().startsWith("_sc|toolong|"));

            final List<String> messages = server.messagesReceived();
            assertThat(messages, hasItem(comparesEqualTo("_sc|fine|0")));
        }
    }

    @Test(timeout=5000L)
    public void sends_too_large_message() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();


        try (final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
                .prefix("my.prefix")
                .hostname("localhost")
                .port(STATSD_SERVER_PORT)
                .errorHandler(errorHandler)
                .build()) {

            final byte[] messageBytes = new byte[1600];
            final ServiceCheck tooLongServiceCheck = ServiceCheck.builder()
                    .withName("toolong")
                    .withMessage(new String(messageBytes))
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(tooLongServiceCheck);

            final ServiceCheck withinLimitServiceCheck = ServiceCheck.builder()
                    .withName("fine")
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(withinLimitServiceCheck);

            server.waitForMessage("_sc");

            final List<Exception> exceptions = errorHandler.getExceptions();
            assertEquals(1, exceptions.size());
            final Exception exception = exceptions.get(0);
            assertEquals(InvalidMessageException.class, exception.getClass());
            assertTrue(((InvalidMessageException)exception).getInvalidMessage().startsWith("_sc|toolong|"));
            // assertEquals(BufferOverflowException.class, exception.getClass());

            final List<String> messages = server.messagesReceived();
            assertThat(messages, hasItem(comparesEqualTo("_sc|fine|0")));
        }
    }

    @Test(timeout=5000L)
    public void sends_telemetry_elsewhere() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final DummyStatsDServer telemetryServer = new DummyStatsDServer(STATSD_SERVER_PORT+10);
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .telemetryHostname("localhost")
            .telemetryPort(STATSD_SERVER_PORT+10)
            .telemetryFlushInterval(3000)
            .errorHandler(errorHandler)
            .build();

        try {
            testClient.gauge("top.level.value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.top.level.value:423|g")));

            telemetryServer.waitForMessage();

            // 8 messages in telemetry batch
            final List<String> messages = telemetryServer.messagesReceived();
            assertEquals(8, messages.size());
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.metrics:1|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.events:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.service_checks:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.bytes_sent:31|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.bytes_dropped:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.packets_sent:1|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.packets_dropped:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.packets_dropped_queue:0|c")));
        } finally {
            testClient.stop();
            telemetryServer.close();
        }
    }

    @Test(timeout = 5000L)
    public void shutdown_test() throws Exception {
        final int port = 17256;
        final int qSize = 256;
        final DummyStatsDServer server = new DummyStatsDServer(port);

        final NonBlockingStatsDClientBuilder builder = new SlowStatsDNonBlockingStatsDClientBuilder().prefix("")
            .hostname("localhost")
            .port(port);
        final SlowStatsDNonBlockingStatsDClient client = ((SlowStatsDNonBlockingStatsDClientBuilder)builder).build();

        try {
            client.count("mycounter", 5);
            assertEquals(0, server.messagesReceived().size());
            server.waitForMessage();
            assertEquals(1, server.messagesReceived().size());
        } finally {
            client.stop();
            server.close();
            assertEquals(0, client.getLock().getCount());
        }
    }

    private static class SlowStatsDNonBlockingStatsDClient extends NonBlockingStatsDClient {

        private CountDownLatch lock;

        SlowStatsDNonBlockingStatsDClient(final String prefix,  final int queueSize,
                String[] constantTags, final StatsDClientErrorHandler errorHandler,
                Callable<SocketAddress> addressLookup, final int timeout, final int bufferSize,
                final int maxPacketSizeBytes, String entityID, final int poolSize, final int processorWorkers,
                final int senderWorkers, boolean blocking) throws StatsDClientException {

            super(prefix, queueSize, constantTags, errorHandler, addressLookup, addressLookup, timeout,bufferSize,
                    maxPacketSizeBytes, entityID, poolSize, processorWorkers, senderWorkers, blocking, false, 0, 0);
            lock = new CountDownLatch(1);
        }

        public CountDownLatch getLock() {
            return this.lock;
        }

        @Override
        public void stop() {
            super.stop();
            lock.countDown();
        }

    };

    private static class SlowStatsDNonBlockingStatsDClientBuilder extends NonBlockingStatsDClientBuilder {

        @Override
        public SlowStatsDNonBlockingStatsDClient build() throws StatsDClientException {
            if (addressLookup != null) {
                return new SlowStatsDNonBlockingStatsDClient(prefix, queueSize, constantTags, errorHandler,
                        addressLookup, timeout, socketBufferSize, maxPacketSizeBytes, entityID, bufferPoolSize,
                        processorWorkers, senderWorkers, blocking);
            } else {
                return new SlowStatsDNonBlockingStatsDClient(prefix, queueSize, constantTags, errorHandler,
                        staticStatsDAddressResolution(hostname, port), timeout, socketBufferSize, maxPacketSizeBytes,
                        entityID, bufferPoolSize, processorWorkers, senderWorkers, blocking);
            }
        }
    }
}
