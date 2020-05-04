# Java DogStatsD Client

[![Build Status](https://travis-ci.com/DataDog/java-dogstatsd-client.svg?branch=master)](https://travis-ci.com/DataDog/java-dogstatsd-client)

A StatsD client library implemented in Java. Allows for Java applications to easily communicate with statsd.

This version was originally forked from [java-dogstatsd-client](https://github.com/indeedeng/java-dogstatsd-client) and [java-statsd-client](https://github.com/youdevise/java-statsd-client) but it is now the canonical home for the `java-dogstatsd-client`. Collaborating with the former upstream projects we have now combined efforts to provide a single release.

See [CHANGELOG.md](CHANGELOG.md) for changes.

## Installation

The client jar is distributed via Maven central, and can be downloaded [from Maven](http://search.maven.org/#search%7Cga%7C1%7Cg%3Acom.datadoghq%20a%3Ajava-dogstatsd-client).

```xml
<dependency>
    <groupId>com.datadoghq</groupId>
    <artifactId>java-dogstatsd-client</artifactId>
    <version>2.9.0</version>
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

or

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

See the full list of available [DogStatsD Client instantiation parameters](https://docs.datadoghq.com/developers/dogstatsd/?tab=java#client-instantiation-parameters).

### Origin detection over UDP and UDS

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


## Usage

In order to use DogStatsD metrics, events, and Service Checks the Agent must be [running and available](https://docs.datadoghq.com/developers/dogstatsd/?tab=java).

### Metrics

After the client is created, you can start sending custom metrics to Datadog. See the dedicated [Metric Submission: DogStatsD documentation](https://docs.datadoghq.com/developers/metrics/dogstatsd_metrics_submission/?tab=java) to see how to submit all supported metric types to Datadog with working code examples:

* [Submit a COUNT metric](https://docs.datadoghq.com/developers/metrics/dogstatsd_metrics_submission/?tab=java#count).
* [Submit a GAUGE metric](https://docs.datadoghq.com/developers/metrics/dogstatsd_metrics_submission/?tab=java#gauge).
* [Submit a HISTOGRAM metric](https://docs.datadoghq.com/developers/metrics/dogstatsd_metrics_submission/?tab=java#histogram)
* [Submit a DISTRIBUTION metric](https://docs.datadoghq.com/developers/metrics/dogstatsd_metrics_submission/?tab=java#distribution)

Some options are suppported when submitting metrics, like [applying a Sample Rate to your metrics](https://docs.datadoghq.com/developers/metrics/dogstatsd_metrics_submission/?tab=java#metric-submission-options) or [tagging your metrics with your custom tags](https://docs.datadoghq.com/developers/metrics/dogstatsd_metrics_submission/?tab=java#metric-tagging).

### Events

After the client is created, you can start sending events to your Datadog Event Stream. See the dedicated [Event Submission: DogStatsD documentation](https://docs.datadoghq.com/developers/events/dogstatsd/?tab=java) to see how to submit an event to your Datadog Event Stream.

### Service Checks

After the client is created, you can start sending Service Checks to Datadog. See the dedicated [Service Check Submission: DogStatsD documentation](https://docs.datadoghq.com/developers/service_checks/dogstatsd_service_checks_submission/?tab=java) to see how to submit a Service Check to Datadog.
