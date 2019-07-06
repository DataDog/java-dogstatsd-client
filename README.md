# Java DogStatsD Client

[![Build Status](https://travis-ci.com/DataDog/java-dogstatsd-client.svg?branch=master)](https://travis-ci.org/DataDog/java-dogstatsd-client)

A statsd client library implemented in Java.  Allows for Java applications to easily communicate with statsd.

This version was originally forked from [java-dogstatsd-client](https://github.com/indeedeng/java-dogstatsd-client) and [java-statsd-client](https://github.com/youdevise/java-statsd-client) but it is now the canonical home for the java-dogstatsd-client.  Collaborating with the former upstream projects we have now combined efforts to provide a single release.

See [CHANGELOG.md](CHANGELOG.md) for changes.

## Downloads

The client jar is distributed via maven central, and can be downloaded [here](http://search.maven.org/#search%7Cga%7C1%7Cg%3Acom.datadoghq%20a%3Ajava-dogstatsd-client).

```xml
<dependency>
    <groupId>com.datadoghq</groupId>
    <artifactId>java-dogstatsd-client</artifactId>
    <version>2.8</version>
</dependency>
```

## Usage

```java
import com.timgroup.statsd.ServiceCheck;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;

public class Foo {

  private static final StatsDClient statsd = new NonBlockingStatsDClient(
    "my.prefix",                          /* prefix to any stats; may be null or empty string */
    "statsd-host",                        /* common case: localhost */
    8125,                                 /* port */
    new String[] {"tag:value"}            /* Datadog extension: Constant tags, always applied */
  );

  public static final void main(String[] args) {
    statsd.incrementCounter("foo");
    statsd.recordGaugeValue("bar", 100);
    statsd.recordGaugeValue("baz", 0.01); /* DataDog extension: support for floating-point gauges */
    statsd.recordHistogramValue("qux", 15);     /* DataDog extension: histograms */
    statsd.recordHistogramValue("qux", 15.5);   /* ...also floating-point */
    statsd.recordDistributionValue("qux", 15);     /* DataDog extension: global distributions */
    statsd.recordDistributionValue("qux", 15.5);   /* ...also floating-point */

    ServiceCheck sc = ServiceCheck
          .builder()
          .withName("my.check.name")
          .withStatus(ServiceCheck.Status.OK)
          .build();
    statsd.serviceCheck(sc); /* Datadog extension: send service check status */

    statsd.recordExecutionTime("bag", 25, "cluster:foo"); /* DataDog extension: cluster tag */
  }
}
```

Unix Domain Socket support
---------------------------

As an alternative to UDP, Agent6 can receive metrics via a UNIX Socket (on Linux only). This library supports
transmission via this protocol. To use it, simply pass the socket path as a hostname, and 0 as port.

By default, all exceptions are ignored, mimicking UDP behaviour. When using Unix Sockets, transmission errors will
trigger exceptions you can choose to handle by passing a `StatsDClientErrorHandler`:

- Connection error because of an invalid/missing socket will trigger a `java.io.IOException: No such file or directory`
- If dogstatsd's reception buffer were to fill up, the send will timeout after 100ms and throw either a
`java.io.IOException: No buffer space available` or a `java.io.IOException: Resource temporarily unavailable`
