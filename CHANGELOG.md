# CHANGELOG

## 4.4.0 / 2024.05.24

* [FEATURE] Added new API to send multiple samples at once. See [#235]
* [BUGFIX] When using `SOCK_STREAM` Unix sockets we now correctly close the socket. See [#243][]
* [IMPROVEMENT] Various performance optimizations. See [#241][]

## 4.3.0 / 2024.01.24

* [FEATURE] Add support for `SOCK_STREAM` Unix sockets. See [#228][]

## 4.2.1 / 2023.03.10

* [FEATURE] Add support for `DD_DOGSTATSD_URL`. See [#217][]

## 4.2.0 / 2023.01.23

* [FEATURE] Sending metrics with a timestamp. See [#211][]
* [IMPROVEMENT] Various performance optimizations. See [#203][]

## 4.1.0 / 2022.10.06

* [FEATURE] Client-side origin detection. See [#188][]
* [BUGFIX] Don't report the expected InterruptedException. See [#193][]
* [BUGFIX] Fix performance issue in the aggregator. See [#194][] (Thanks [@retronym][])
* [BUGFIX] Clear buffers before returning them to the pool. See [#200][]
* [BUGFIX] Fix high CPU usage during client shutdown. See [#201][]

## 4.0.0 / 2022.01.10

This release is a correction for v3.0.0, which was released without client-side
aggregation enabled by default.

There are no incompatible API changes in this release.

* [BUGFIX] Re-enable aggregation by default. See [#171][]
* [FEATURE] Windows named pipe support. See [#169][]
* [FEATURE] jar-with-dependencies artifact now include license information. See [#179][]
* [FEATURE] send buffered metrics when a blocking client is closed. See [#180][]
* [FEATURE] client can be used without jnr-posix in the classpath. See [#182][]

## 3.0.1 / 2021.12.14

* [BUGFIX] Fix visibility for overrideable methods. See [#170][]

## 3.0.0 / 2021.11.02

This release marks a new major release, and includes some *breaking changes*.
Most notably:
- Client aggregation enabled by default for simple types.
- Client aggregation flush interval changed to 2s.
- Internal client telemetry metrics are now included in the client telemetry by default.
- Removal of most overloaded constructors.

Many users will be able to upgrade seamlessly, while some might be required to make
changes due to the removal of an excessively overladed constructor anti-pattern.
Please refer to the [readme][readme configuration] for tips on how to migrate to
`v3.x` builder pattern to instantiate your client.


* [DEPRECATE] Removing deprecated constructors. See [#158][]
* [FEATURE] Client aggregation enabled by default. See [#164][]
* [FEATURE] Allow clients to override metric sampling. See [#162][]
* [FEATURE] Client internal metrics included in telemetry by default. See [#157][]
* [BUGFIX] Set client side aggregation flush interval to 2s. See [#154][]
* [BUGFIX] Make text in events non-mandatory. See [#160][]
* [DOCS] Make text in events non-mandatory. See [#160][]


## 2.13.0 / 2021.06.09

* [FEATURE] Telemetry: adding developer mode: additional metrics. See [#131][]
* [IMPROVEMENT] Cleanup internal thread model. See [#144][]
* [IMPROVEMENT] All dogstatsd messages are EOL terminated. See [#130][]
* [IMPROVEMENT] Refactor version.properties to dedicated directory. See [#147][] (Thanks [@cameronhotchkies][])
* [IMPROVEMENT] Dev: make environment variable variables public. See [#132][] (Thanks [@dbyron0][])
* [BUGFIX] Properly compute unicode event strings length. See [#149][]
* [DOCUMENTATION] Updated javadoc and README. See [#139][] and [#136][]. (Thanks [@snopoke][] and [@gherceg][])


## 2.12.0 / 2021.04.05

* [IMPROVEMENT] Deployments to Sonatype are now automated (but manually triggered).
  No user-facing changes are part of this release.

## 2.11.0 / 2020.12.22

* [FEATURE] Aggregation: simple type client-side aggregation. See [#121][]
* [IMPROVEMENT] UDP+UDS: set better defaults for max packet size. See [#125][]
* [BUGFIX] Aggregator: fix thread leak + dont always start scheduler. See [#129][]
* [BUGFIX] Sampling: on counts to be disabled when aggregation is enabled. See [#127][]
* [BUGFIX] Processor: shutdown the executor on cue, dont leak. See [#126][]
* [DOCS] Aggregation: update with new aggregation instructions. See [#122][]

## 2.10.3 / 2020.07.17
* [BUGFIX] Fix library shutdown: use daemon threads for StatsDProcess + TimerTask. See [#117][] (Thanks [@blevz][])

## 2.10.2 / 2020.07.07
* [BUGFIX] Fix thread leak on shutdown: release StatsDSender executor. See [#115][] (Thanks [@hanny24][])

## 2.10.1 / 2020.05.26
* [BUGFIX] Fixes build issue on JDK8. No code changes.

## 2.10.0 / 2020.05.04
* [FEATURE] Architecture revamp + non-blocking queue, improved performance. See [#94][]
* [FEATURE] Enable buffer pool, concurrent sending threads. See [#95][]
* [FEATURE] Adding dogstatsd telemetry to client. See [#97][]
* [FEATURE] DD_SERVICE; DD_ENV; DD_VERSION env vars support. See [#107][], [#108][], [#111][]
* [FEATURE] Allow different remote destination for telemetry. See [#109][]
* [IMPROVEMENT] Improved abstractions + better object construction. See [#96][]
* [IMPROVEMENT] Reduce number of allocations. Thanks [@njhill][]. See [#105][]
* [DOCS] Container specific tags. See [#110][]

## 2.9.0 / 2020.02.20
* [FIX] Add source type name to event payload. See [#101][]
* [IMPROVEMENT] Bump jnr-unixsocket to 0.27. See [#102][]
* [IMPROVEMENT] Bump maven-compiler-plugin to 3.8.1. See [#93][]
* [DOCS] Multiple documentation updates. See [#91][]

## 2.8.1 / 2019.10.4
* [FIX] Fix entity id with constant tags

## 2.8.0 / 2019.06.18
* [FEATURE] Support environment variables for client configuration
* [FIX] Handle messages over max packet size limit
* [FEATURE] Take an argument for maxPacketSizeBytes

## 2.7.1 / 2019.01.22

* [FEATURE] Ability to configure unix buffer timeout and size. See [#64][]
* [FIX] Catch any Socket IO Exception on test cleanup. See [#67][]

## 2.6.1 / 2018.07.06

* [BUGFIX] Fix older Java compatability issues due to building with newer versions of Java that affected v2.6 of this library.
* [DEV] Enforce Java 7/8 when testing/building. See [#53][].

## 2.6 / 2018.06.29

* [FEATURE] Add support for submitting data through Unix Domain Sockets. See [#42][].
* [IMPROVEMENT] Replace string `format` with `StringBuilder`for performance gains. Thanks [@cithal][] See [#49][].

## 2.5 / 2018.01.23

* Added support for new beta feature, global distributions

## 2.4 / 2017.12.12

Note: Starting from this version the client requires Java7+

* Support for floating-point delta in counters, see [#37][]
* Use ThreadLocalRandom vs Math.random, see [#16][]
* Fix maven warnings about versions and encoding, see [#34][]
* Add close method to StatsDClient and extend Closeable, see [#33][]

## 2.3 / 2016.10.21

* [BUGFIX] Reduce packet size to avoid fragmentation, see [#17](https://github.com/DataDog/java-dogstatsd-client/pull/17).
* [BUGFIX] Prefix sample rate with an '@', see [#15](https://github.com/DataDog/java-dogstatsd-client/pull/15).
* [FEATURE] Add support for sending `sample_rate` while submitting metrics, see [#11](https://github.com/DataDog/java-dogstatsd-client/pull/11).

## 2.2 / 2016.07.13

* [OTHER] Merged upstream from `indeedeng/java-dogstatsd-client`

## 2.1.1 / 2015.04.27

* [BUGFIX] Constant tag support for service checks. See [#3][], thanks [@PatrickAuld][]
* [OTHER] Changelog update

## 2.1.0 / 2015.03.12

Fork from [indeedeng/java-dogstatsd-client] (https://github.com/indeedeng/java-dogstatsd-client/releases/tag/java-dogstatsd-client-2.0.7) statsd client library
* [FEATURE] Add support for Datadog service checks


[readme configuration]: https://github.com/DataDog/java-dogstatsd-client/blob/master/README.md#configuration


<!--- The following link definition list is generated by PimpMyChangelog --->
[#3]: https://github.com/DataDog/java-dogstatsd-client/issues/3
[#11]: https://github.com/DataDog/java-dogstatsd-client/issues/11
[#15]: https://github.com/DataDog/java-dogstatsd-client/issues/15
[#16]: https://github.com/DataDog/java-dogstatsd-client/issues/16
[#17]: https://github.com/DataDog/java-dogstatsd-client/issues/17
[#33]: https://github.com/DataDog/java-dogstatsd-client/issues/33
[#34]: https://github.com/DataDog/java-dogstatsd-client/issues/34
[#37]: https://github.com/DataDog/java-dogstatsd-client/issues/37
[#42]: https://github.com/DataDog/java-dogstatsd-client/issues/42
[#49]: https://github.com/DataDog/java-dogstatsd-client/issues/49
[#53]: https://github.com/DataDog/java-dogstatsd-client/issues/53
[#64]: https://github.com/DataDog/java-dogstatsd-client/issues/64
[#67]: https://github.com/DataDog/java-dogstatsd-client/issues/67
[#91]: https://github.com/DataDog/java-dogstatsd-client/issues/91
[#93]: https://github.com/DataDog/java-dogstatsd-client/issues/93
[#94]: https://github.com/DataDog/java-dogstatsd-client/issues/94
[#95]: https://github.com/DataDog/java-dogstatsd-client/issues/95
[#96]: https://github.com/DataDog/java-dogstatsd-client/issues/96
[#97]: https://github.com/DataDog/java-dogstatsd-client/issues/97
[#101]: https://github.com/DataDog/java-dogstatsd-client/issues/101
[#102]: https://github.com/DataDog/java-dogstatsd-client/issues/102
[#105]: https://github.com/DataDog/java-dogstatsd-client/issues/105
[#107]: https://github.com/DataDog/java-dogstatsd-client/issues/107
[#108]: https://github.com/DataDog/java-dogstatsd-client/issues/108
[#109]: https://github.com/DataDog/java-dogstatsd-client/issues/109
[#110]: https://github.com/DataDog/java-dogstatsd-client/issues/110
[#111]: https://github.com/DataDog/java-dogstatsd-client/issues/111
[#115]: https://github.com/DataDog/java-dogstatsd-client/issues/115
[#117]: https://github.com/DataDog/java-dogstatsd-client/issues/117
[#121]: https://github.com/DataDog/java-dogstatsd-client/issues/121
[#122]: https://github.com/DataDog/java-dogstatsd-client/issues/122
[#125]: https://github.com/DataDog/java-dogstatsd-client/issues/125
[#126]: https://github.com/DataDog/java-dogstatsd-client/issues/126
[#127]: https://github.com/DataDog/java-dogstatsd-client/issues/127
[#129]: https://github.com/DataDog/java-dogstatsd-client/issues/129
[#130]: https://github.com/DataDog/java-dogstatsd-client/issues/130
[#131]: https://github.com/DataDog/java-dogstatsd-client/issues/131
[#132]: https://github.com/DataDog/java-dogstatsd-client/issues/132
[#136]: https://github.com/DataDog/java-dogstatsd-client/issues/136
[#139]: https://github.com/DataDog/java-dogstatsd-client/issues/139
[#144]: https://github.com/DataDog/java-dogstatsd-client/issues/144
[#147]: https://github.com/DataDog/java-dogstatsd-client/issues/147
[#149]: https://github.com/DataDog/java-dogstatsd-client/issues/149
[#154]: https://github.com/DataDog/java-dogstatsd-client/issues/154
[#157]: https://github.com/DataDog/java-dogstatsd-client/issues/157
[#158]: https://github.com/DataDog/java-dogstatsd-client/issues/158
[#160]: https://github.com/DataDog/java-dogstatsd-client/issues/160
[#162]: https://github.com/DataDog/java-dogstatsd-client/issues/162
[#164]: https://github.com/DataDog/java-dogstatsd-client/issues/164
[#169]: https://github.com/DataDog/java-dogstatsd-client/issues/169
[#170]: https://github.com/DataDog/java-dogstatsd-client/issues/170
[#171]: https://github.com/DataDog/java-dogstatsd-client/issues/171
[#179]: https://github.com/DataDog/java-dogstatsd-client/issues/179
[#180]: https://github.com/DataDog/java-dogstatsd-client/issues/180
[#182]: https://github.com/DataDog/java-dogstatsd-client/issues/182
[#188]: https://github.com/DataDog/java-dogstatsd-client/issues/188
[#193]: https://github.com/DataDog/java-dogstatsd-client/issues/193
[#194]: https://github.com/DataDog/java-dogstatsd-client/issues/194
[#200]: https://github.com/DataDog/java-dogstatsd-client/issues/200
[#201]: https://github.com/DataDog/java-dogstatsd-client/issues/201
[#203]: https://github.com/DataDog/java-dogstatsd-client/issues/203
[#211]: https://github.com/DataDog/java-dogstatsd-client/issues/211
[#217]: https://github.com/DataDog/java-dogstatsd-client/issues/217
[#228]: https://github.com/DataDog/java-dogstatsd-client/issues/228
[#235]: https://github.com/DataDog/java-dogstatsd-client/issues/235
[#241]: https://github.com/DataDog/java-dogstatsd-client/issues/241
[#243]: https://github.com/DataDog/java-dogstatsd-client/issues/243

[@PatrickAuld]: https://github.com/PatrickAuld
[@blevz]: https://github.com/blevz
[@cameronhotchkies]: https://github.com/cameronhotchkies
[@cithal]: https://github.com/cithal
[@dbyron0]: https://github.com/dbyron0
[@gherceg]: https://github.com/gherceg
[@hanny24]: https://github.com/hanny24
[@njhill]: https://github.com/njhill
[@snopoke]: https://github.com/snopoke
[@retronym]: https://github.com/retronym
