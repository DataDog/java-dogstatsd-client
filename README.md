# Java DogStatsD Client

[![CircleCI Build Status](https://circleci.com/gh/DataDog/java-dogstatsd-client/tree/master.svg?style=svg)](https://circleci.com/gh/DataDog/java-dogstatsd-client)
[![Travis Build Status](https://travis-ci.com/DataDog/java-dogstatsd-client.svg?branch=master)](https://travis-ci.com/DataDog/java-dogstatsd-client)

A DogStatsD client library implemented in Java. Allows for Java applications to easily communicate with the DataDog Agent. The library supports Java 1.7+.

This version was originally forked from [java-dogstatsd-client](https://github.com/indeedeng/java-dogstatsd-client) and [java-statsd-client](https://github.com/youdevise/java-statsd-client) but it is now the canonical home for the `java-dogstatsd-client`. Collaborating with the former upstream projects we have now combined efforts to provide a single release.

See [CHANGELOG.md](CHANGELOG.md) for changes.

## Installation

The client jar is distributed via Maven central, and can be downloaded [from Maven](http://search.maven.org/#search%7Cga%7C1%7Cg%3Acom.datadoghq%20a%3Ajava-dogstatsd-client).

```xml
<dependency>
    <groupId>com.datadoghq</groupId>
    <artifactId>java-dogstatsd-client</artifactId>
    <version>4.1.0</version>
</dependency>
```

### Unix Domain Socket support

As an alternative to UDP, Agent v6 can receive metrics via a UNIX Socket (on Linux only). This library supports transmission via this protocol. To use it, pass the socket path as a hostname, and `0` as port.

By default, all exceptions are ignored, mimicking UDP behaviour. When using Unix Sockets, transmission errors trigger exceptions you can choose to handle by passing a `StatsDClientErrorHandler`:

- Connection error because of an invalid/missing socket triggers a `java.io.IOException: No such file or directory`.
- If DogStatsD's reception buffer were to fill up and the non blocking client is used, the send times out after 100ms and throw either a `java.io.IOException: No buffer space available` or a `java.io.IOException: Resource temporarily unavailable`.

## Configuration

Once your DogStatsD client is installed, instantiate it in your code:

```java
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class DogStatsdClient {

    public static void main(String[] args) throws Exception {

        StatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("statsd")
            .hostname("localhost")
            .port(8125)
            .build();

        // use your client...
    }
}
```

alternatively:

```java
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class DogStatsdClient {

    public static void main(String[] args) throws Exception {

        // resolve() returns a copy of NonBlockingStatsDClient with
        // any implicit elements resolved, and may also be passed to
        // NonBlockingStatsDClient to as a sole constructor argument.

        StatsDClient client = new NonBlockingStatsDClient(
            new NonBlockingStatsDClientBuilder()
                .prefix("statsd")
                .hostname("localhost")
                .port(8125)
                .resolve());

        // use your client...
    }
}
```

### v2.x

Client version `v3.x` is now preferred over the older client `v2.x` release line. We do suggest you upgrade to the newer `v3.x` at
your earliest convenience.

The builder pattern described above was introduced with `v2.10.0` in the `v2.x` series. Earlier releases require you use the
deprecated overloaded constructors.


**DEPRECATED**
```java
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class DogStatsdClient {

    public static void main(String[] args) throws Exception {

        StatsDClient Statsd = new NonBlockingStatsDClient("statsd", "localhost", 8125);

    }
}
```

See the full list of available [DogStatsD Client instantiation parameters](https://docs.datadoghq.com/developers/dogstatsd/?tab=hostagent#client-instantiation-parameters).

### Migrating from 2.x to 3.x

Though there are very few breaking changes in `3.x`, some code changes might be required for some user to migrate to the latest version. If you are migrating from the `v2.x` series to `v3.x` and were using the deprecated constructors, please use the following table to ease your migration to utilizing the builder pattern.

| v2.x constructor parameter | v2.10.0+ builder method |
|----------------------------|-------------------------|
|  final Callable<SocketAddress> addressLookup                         | NonBlockingStatsDClientBuilder addressLookup(Callable<SocketAddress> val)                         |
|  final boolean blocking                          | NonBlockingStatsDClientBuilder blocking(boolean val)                         |
|  final int bufferSize                          | NonBlockingStatsDClientBuilder socketBufferSize(int val)                         |
|  final String... constantTags                          | NonBlockingStatsDClientBuilder constantTags(String... val)                         |
|  final boolean enableTelemetry                          | NonBlockingStatsDClientBuilder enableTelemetry(boolean val)                         |
|  final String entityID                          | NonBlockingStatsDClientBuilder entityID(String val)                         |
|  final StatsDClientErrorHandler errorHandler                          | NonBlockingStatsDClientBuilder errorHandler(StatsDClientErrorHandler val)                         |
|  final String hostname                          | NonBlockingStatsDClientBuilder hostname(String val)                         |
|  final int maxPacketSizeBytes                          | NonBlockingStatsDClientBuilder maxPacketSizeBytes(String... val)                         |
|  final int processorWorkers                          | NonBlockingStatsDClientBuilder processorWorkers(int val)                         |
|  final int poolSize                          | NonBlockingStatsDClientBuilder bufferPoolSize(int val)                         |
|  final int port                          | NonBlockingStatsDClientBuilder port(int val)                         |
|  final String prefix                          | NonBlockingStatsDClientBuilder prefix(String val)                         |
|  final int queueSize                          | NonBlockingStatsDClientBuilder queueSize(int val)                         |
|  final int senderWorkers                          | NonBlockingStatsDClientBuilder senderWorkers(int val)                         |
|  final Callable<SocketAddress> telemetryAddressLookup                         | NonBlockingStatsDClientBuilder telemetryAddressLookup(Callable<SocketAddress> val)                         |
|  final int telemetryFlushInterval                          | NonBlockingStatsDClientBuilder telemetryFlushInterval(int val)                         |
|  final int timeout                          | NonBlockingStatsDClientBuilder timeout(int val)                         |

### Transport and Maximum Packet Size

As mentioned above the client currently supports two forms of transport: UDP and Unix Domain Sockets (UDS).

The preferred setup for local transport is UDS, while remote setups will require the use of UDP. For both setups we have tried to set convenient maximum default packet sizes that should help with performance by packing multiple statsd metrics into each network packet all while playing nicely with the respective environments. For this reason we have set the following defaults for the max packet size:
- UDS: 8192 bytes - recommended default.
- UDP: 1432 bytes - largest possible size given the Ethernet MTU of 1514 Bytes. This should help avoid UDP fragmentation.

These are both configurable should you have other needs:
```java
StatsDClient client = new NonBlockingStatsDClientBuilder()
    .hostname("/var/run/datadog/dsd.socket")
    .maxPacketSizeBytes(16384)  // 16kB maximum custom value
    .build();
```

#### Origin detection over UDP and UDS

Origin detection is a method to detect which pod `DogStatsD` packets are coming from in order to add the pod's tags to the tag list.
The `DogStatsD` client attaches an internal tag, `entity_id`. The value of this tag is the content of the `DD_ENTITY_ID` environment variable if found, which is the pod's UID. The Datadog Agent uses this tag to add container tags to the metrics. To avoid overwriting this global tag, make sure to only `append` to the `constant_tags` list.

To enable origin detection over UDP, add the following lines to your application manifest
```yaml
env:
  - name: DD_ENTITY_ID
    valueFrom:
      fieldRef:
        fieldPath: metadata.uid
```

## Aggregation

As of version `v2.11.0`, client-side aggregation has been introduced in the java client side for basic types (gauges, counts, sets). Aggregation remains unavailable at the
time of this writing for histograms, distributions, service checks and events due to message relevance and statistical significance of these types. The feature is enabled by default as of `v3.0.0`, and remains available but disabled by default for prior versions.

The goal of this feature is to reduce the number of messages submitted to the Datadog Agent. Minimizing message volume allows us to reduce load on the dogstatsd server side
and mitigate packet drops. The feature has been implemented such that impact on CPU and memory should be quite minimal on the client side. Users might be concerned with
what could be perceived as a loss of resolution by resorting to aggregation on the client, this should not be the case. It's worth noting the dogstatsd server implemented
in the Datadog Agent already aggregates messages over a certain flush period, therefore so long as the flush interval configured on the client side is smaller
than said flush interval on the server side there should no loss in resolution.

### Configuration

Enabling aggregation is simple, you just need to set the appropriate options with the client builder.

You can just enable aggregation by calling the `enableAggregation(bool)` method on the builder.

There are two clent-side aggregation knobs available:
- `aggregationShards(int)`: determines the number of shards in the aggregator, this
 feature is aimed at mitigating the effects of map locking in highly concurrent scenarios. Defaults to 4.
- `aggregationFlushInterval(int)`: sets the period of time in milliseconds in which the
aggregator will flush its metrics into the sender. Defaults to 3000 milliseconds.

```java
StatsDClient client = new NonBlockingStatsDClientBuilder()
    .hostname("localhost")
    .port(8125)
    .enableAggregation(true)
    .aggregationFlushInterval(3000)  // optional: in milliseconds
    .aggregationShards(8)  // optional: defaults to 4
    .build();
```

## Usage

In order to use DogStatsD metrics, events, and Service Checks the Agent must be [running and available](https://docs.datadoghq.com/developers/dogstatsd/).

### Metrics

After the client is created, you can start sending custom metrics to Datadog. See the dedicated [Metric Submission: DogStatsD documentation](https://docs.datadoghq.com/metrics/dogstatsd_metrics_submission/) to see how to submit all supported metric types to Datadog with working code examples:

* [Submit a COUNT metric](https://docs.datadoghq.com/metrics/dogstatsd_metrics_submission/#count).
* [Submit a GAUGE metric](https://docs.datadoghq.com/metrics/dogstatsd_metrics_submission/#gauge).
* [Submit a HISTOGRAM metric](https://docs.datadoghq.com/metrics/dogstatsd_metrics_submission/#histogram)
* [Submit a DISTRIBUTION metric](https://docs.datadoghq.com/metrics/dogstatsd_metrics_submission/#distribution)

Some options are suppported when submitting metrics, like [applying a Sample Rate to your metrics](https://docs.datadoghq.com/metrics/dogstatsd_metrics_submission/#metric-submission-options) or [tagging your metrics with your custom tags](https://docs.datadoghq.com/metrics/dogstatsd_metrics_submission/#metric-tagging).

### Events

After the client is created, you can start sending events to your Datadog Event Stream. See the dedicated [Event Submission: DogStatsD documentation](https://docs.datadoghq.com/developers/events/dogstatsd/) to see how to submit an event to your Datadog Event Stream.

### Service Checks

After the client is created, you can start sending Service Checks to Datadog. See the dedicated [Service Check Submission: DogStatsD documentation](https://docs.datadoghq.com/developers/service_checks/dogstatsd_service_checks_submission/) to see how to submit a Service Check to Datadog.
