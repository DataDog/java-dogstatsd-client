/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.datadoghq.dogstatsd.Sketch;
import com.datadoghq.dogstatsd.http.serializer.PayloadBuilder;
import com.datadoghq.dogstatsd.http.serializer.PayloadConsumer;
import com.datadoghq.dogstatsd.http.serializer.TagsCardinality;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class DirectHttpClientTest {
    private static final URI seriesUri = URI.create("series");
    private static final URI sketchesUri = URI.create("sketches");

    static class TestForwarder implements DirectHttpClient.Forwarder {
        final ArrayList<URI> uris = new ArrayList<>();
        final ArrayList<byte[]> payloads = new ArrayList<>();

        @Override
        public void send(URI uri, byte[] payload) {
            uris.add(uri);
            payloads.add(payload);
        }
    }

    private static PayloadBuilder builderInto(final List<byte[]> payloads) {
        return new PayloadBuilder(
                new PayloadConsumer() {
                    @Override
                    public void handle(byte[] p) {
                        payloads.add(p);
                    }
                });
    }

    /** Asserts the client sent exactly one payload to uri, matching the only expected payload. */
    private static void assertSent(List<byte[]> expected, TestForwarder fwd, URI uri, String what) {
        assertEquals("expected payloads", 1, expected.size());
        assertEquals("payloads sent", Collections.singletonList(uri), fwd.uris);
        assertArrayEquals(what, expected.get(0), fwd.payloads.get(0));
    }

    @Test
    public void gaugeSendsHostTagAsResource() {
        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).prefix("pfx").build();
        client.gauge("metric", 1.5, 100, Arrays.asList("a:b", "host:h1", "host:h2"));
        client.flush();

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.gauge("pfx.metric")
                .setTags(Collections.singletonList("a:b"))
                .setResources(Arrays.asList("host", "h1"))
                .setInterval(10)
                .addPoint(100, 1.5)
                .close();
        b.close();

        assertSent(expected, fwd, seriesUri, "gauge");
    }

    @Test
    public void countSendsHostTagAsResource() {
        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.count("metric", 20, 100, Arrays.asList("host:h1", "a:b"));
        client.flush();

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.rate("metric")
                .setTags(Collections.singletonList("a:b"))
                .setResources(Arrays.asList("host", "h1"))
                .setInterval(10)
                .addPoint(100, 2)
                .close();
        b.close();

        assertSent(expected, fwd, seriesUri, "count");
    }

    @Test
    public void distributionSendsHostTagAsResource() {
        double[] values = new double[] {1, 2, 2};

        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.distribution("metric", values, 1.0, 100, Arrays.asList("host:h1", "a:b"));
        client.flush();

        Sketch sketch = new Sketch();
        sketch.build(values, 1.0);

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.sketch("metric")
                .setTags(Collections.singletonList("a:b"))
                .setResources(Arrays.asList("host", "h1"))
                .addPoint(100, sketch)
                .close();
        b.close();

        assertSent(expected, fwd, sketchesUri, "distribution");
    }

    @Test
    public void noHostTagLeavesResourcesUnset() {
        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.gauge("metric", 1.5, 100, Arrays.asList("a:b", "hostname:h1"));
        client.flush();

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.gauge("metric")
                .setTags(Arrays.asList("a:b", "hostname:h1"))
                .setInterval(10)
                .addPoint(100, 1.5)
                .close();
        b.close();

        assertSent(expected, fwd, seriesUri, "gauge without host tag");
    }

    @Test
    public void emptyHostTagSendsEmptyHostResource() {
        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.gauge("metric", 1.5, 100, Arrays.asList("a:b", "host:"));
        client.flush();

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.gauge("metric")
                .setTags(Collections.singletonList("a:b"))
                .setResources(Arrays.asList("host", ""))
                .setInterval(10)
                .addPoint(100, 1.5)
                .close();
        b.close();

        assertSent(expected, fwd, seriesUri, "gauge with empty host tag");

        // An empty host resource must still be encoded, unlike a metric with no resources at all.
        ArrayList<byte[]> noResources = new ArrayList<>();
        PayloadBuilder nb = builderInto(noResources);
        nb.gauge("metric")
                .setTags(Collections.singletonList("a:b"))
                .setInterval(10)
                .addPoint(100, 1.5)
                .close();
        nb.close();

        assertFalse(
                "empty host resource encodes the same as no resources",
                Arrays.equals(noResources.get(0), fwd.payloads.get(0)));
    }

    @Test
    public void hostTagPicksFirstValue() {
        assertNull(DirectHttpClient.hostTag(null));
        assertNull(DirectHttpClient.hostTag(Collections.<String>emptyList()));
        assertNull(DirectHttpClient.hostTag(Arrays.asList("a:b", "hostname:h1", "hos:t")));
        assertEquals("h1", DirectHttpClient.hostTag(Collections.singletonList("host:h1")));
        assertEquals("h1", DirectHttpClient.hostTag(Arrays.asList("a:b", "host:h1", "host:h2")));
        assertEquals("", DirectHttpClient.hostTag(Collections.singletonList("host:")));
    }

    @Test
    public void withoutHostTagsRemovesEveryHostTag() {
        assertNull(DirectHttpClient.withoutHostTags(null));

        List<String> noHost = Arrays.asList("a:b", "hostname:h1");
        assertSame(noHost, DirectHttpClient.withoutHostTags(noHost));

        assertEquals(
                Collections.<String>emptyList(),
                DirectHttpClient.withoutHostTags(Arrays.asList("host:h1", "host:")));
        assertEquals(
                Arrays.asList("a:b", "c:d"),
                DirectHttpClient.withoutHostTags(
                        Arrays.asList("a:b", "host:h1", "c:d", "host:h2")));

        // The cardinality tag is sent to the agent as a tag as well.
        List<String> card = Arrays.asList("a:b", "dd.internal.card:low");
        assertSame(card, DirectHttpClient.withoutHostTags(card));
    }

    @Test
    public void hostResourceIsATypeNamePair() {
        assertNull(DirectHttpClient.hostResource(null));
        assertEquals(Arrays.asList("host", "h1"), DirectHttpClient.hostResource("h1"));
        assertEquals(Arrays.asList("host", ""), DirectHttpClient.hostResource(""));
    }

    @Test
    public void noCardinalityTagLeavesCardinalityDefault() {
        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.gauge("metric", 1.5, 100, Collections.singletonList("a:b"));
        client.flush();

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.gauge("metric")
                .setTags(Collections.singletonList("a:b"))
                .setTagsCardinality(TagsCardinality.DEFAULT)
                .setInterval(10)
                .addPoint(100, 1.5)
                .close();
        b.close();

        assertSent(expected, fwd, seriesUri, "gauge without cardinality");
    }

    @Test
    public void cardinalityTagIsSentAsCardinality() {
        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.gauge(
                "gauge",
                1.5,
                100,
                Arrays.asList("a:b", "dd.internal.card:high", "host:h1", "dd.internal.card:low"));
        client.count("count", 20, 100, Arrays.asList("dd.internal.card:none", "a:b"));
        client.flush();

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.gauge("gauge")
                .setTags(Arrays.asList("a:b", "dd.internal.card:high", "dd.internal.card:low"))
                .setResources(Arrays.asList("host", "h1"))
                .setTagsCardinality(TagsCardinality.HIGH)
                .setInterval(10)
                .addPoint(100, 1.5)
                .close();
        b.rate("count")
                .setTags(Arrays.asList("dd.internal.card:none", "a:b"))
                .setTagsCardinality(TagsCardinality.NONE)
                .setInterval(10)
                .addPoint(100, 2)
                .close();
        b.close();

        assertSent(expected, fwd, seriesUri, "gauge and count with cardinality tag");
    }

    @Test
    public void distributionSendsCardinalityTagAsCardinality() {
        double[] values = new double[] {1, 2, 2};

        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.distribution(
                "metric", values, 1.0, 100, Arrays.asList("dd.internal.card:orch", "a:b"));
        client.flush();

        Sketch sketch = new Sketch();
        sketch.build(values, 1.0);

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.sketch("metric")
                .setTags(Arrays.asList("dd.internal.card:orch", "a:b"))
                .setTagsCardinality(TagsCardinality.ORCHESTRATOR)
                .addPoint(100, sketch)
                .close();
        b.close();

        assertSent(expected, fwd, sketchesUri, "distribution with cardinality tag");
    }

    @Test
    public void unknownCardinalityTagLeavesCardinalityDefault() {
        TestForwarder fwd = new TestForwarder();
        DirectHttpClient client = DirectHttpClient.builder(fwd).build();
        client.gauge("metric", 1.5, 100, Arrays.asList("a:b", "dd.internal.card:bogus"));
        client.flush();

        ArrayList<byte[]> expected = new ArrayList<>();
        PayloadBuilder b = builderInto(expected);
        b.gauge("metric")
                .setTags(Arrays.asList("a:b", "dd.internal.card:bogus"))
                .setInterval(10)
                .addPoint(100, 1.5)
                .close();
        b.close();

        assertSent(expected, fwd, seriesUri, "gauge with unknown cardinality tag");
    }

    @Test
    public void cardinalityTagPicksFirstValue() {
        assertNull(DirectHttpClient.cardinalityTag(null));
        assertNull(DirectHttpClient.cardinalityTag(Collections.<String>emptyList()));
        assertNull(DirectHttpClient.cardinalityTag(Arrays.asList("a:b", "dd.internal.cardx:low")));
        assertEquals(
                "low",
                DirectHttpClient.cardinalityTag(
                        Arrays.asList("a:b", "dd.internal.card:low", "dd.internal.card:high")));
        assertEquals(
                "",
                DirectHttpClient.cardinalityTag(Collections.singletonList("dd.internal.card:")));
    }

    @Test
    public void cardinalityParsesTheValuesTheAgentAccepts() {
        assertEquals(TagsCardinality.DEFAULT, DirectHttpClient.cardinality(null));
        assertEquals(TagsCardinality.DEFAULT, DirectHttpClient.cardinality(""));
        assertEquals(TagsCardinality.DEFAULT, DirectHttpClient.cardinality("bogus"));
        assertEquals(TagsCardinality.NONE, DirectHttpClient.cardinality("none"));
        assertEquals(TagsCardinality.LOW, DirectHttpClient.cardinality("low"));
        assertEquals(TagsCardinality.ORCHESTRATOR, DirectHttpClient.cardinality("orch"));
        assertEquals(TagsCardinality.ORCHESTRATOR, DirectHttpClient.cardinality("orchestrator"));
        assertEquals(TagsCardinality.HIGH, DirectHttpClient.cardinality("high"));
        // The agent parses the value case-insensitively.
        assertEquals(TagsCardinality.HIGH, DirectHttpClient.cardinality("HIGH"));
    }
}
