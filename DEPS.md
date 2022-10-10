# Overriding dependencies

## Java 8+

As of version v4.1.0, this library can be used with the latest version
of the `jnr-unixsocket` library.

### Maven

```xml
<dependencies>
  <dependency>
    <groupId>com.datadoghq</groupId>
    <artifactId>java-dogstatsd-client</artifactId>
    <version>4.1.0</version>
  </dependency>
  <dependency>
    <groupId>com.github.jnr</groupId>
    <artifactId>jnr-unixsocket</artifactId>
    <version>0.38.17</version>
  </dependency>
</dependencies>
```

### Gradle

```groovy
dependencies {
    implementation('com.datadoghq:java-dogstatsd-client:4.1.0')
    implementation('com.github.jnr:jnr-unixsocket:0.38.17')
}
```

## Without dependencies

As of version v4.1.0, this library can be used without dependencies
when unix sockets support is not required. Trying to instantiate a
client with port set to zero to enable unix socket mode will cause a
`ClassNotFound` exception.

### Maven

```xml
<dependencies>
  <dependency>
    <groupId>com.datadoghq</groupId>
    <artifactId>java-dogstatsd-client</artifactId>
    <version>4.1.0</version>
    <exclusions>
      <exclusion>
        <artifactId>jnr-unixsocket</artifactId>
        <groupId>com.github.jnr</groupId>
      </exclusion>
    </exclusions>
  </dependency>
</dependencies>
```

### Gradle

```groovy
dependencies {
    implementation('com.datadoghq:java-dogstatsd-client:4.1.0') {
        exclude group: 'com.github.jnr', module: 'jnr-unixsocket'
 }
}
```
