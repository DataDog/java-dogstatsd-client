package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public abstract class DefaultStatsDClient implements StatsDClient{

  public static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");
  protected static final int PACKET_SIZE_BYTES = 1400;
  protected static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
      @Override public void handle(final Exception e) { /* No-op */ }
  };
  /**
   * Because NumberFormat is not thread-safe we cannot share instances across threads. Use a ThreadLocal to
   * create one pre thread as this seems to offer a significant performance improvement over creating one per-thread:
   * http://stackoverflow.com/a/1285297/2648
   * https://github.com/indeedeng/java-dogstatsd-client/issues/4
   */
  private static final ThreadLocal<NumberFormat> NUMBER_FORMATTERS = new ThreadLocal<NumberFormat>() {
      @Override
      protected NumberFormat initialValue() {

          // Always create the formatter for the US locale in order to avoid this bug:
          // https://github.com/indeedeng/java-dogstatsd-client/issues/3
          final NumberFormat numberFormatter = NumberFormat.getInstance(Locale.US);
          numberFormatter.setGroupingUsed(false);
          numberFormatter.setMaximumFractionDigits(6);

          // we need to specify a value for Double.NaN that is recognized by dogStatsD
          if (numberFormatter instanceof DecimalFormat) { // better safe than a runtime error
              final DecimalFormat decimalFormat = (DecimalFormat) numberFormatter;
              final DecimalFormatSymbols symbols = decimalFormat.getDecimalFormatSymbols();
              symbols.setNaN("NaN");
              decimalFormat.setDecimalFormatSymbols(symbols);
          }

          return numberFormatter;
      }
  };
  protected final String prefix;
  protected final String constantTagsRendered;
  protected final DatagramChannel clientChannel;
  protected final StatsDClientErrorHandler handler;

  public DefaultStatsDClient(
      final String prefix, String[] constantTags, StatsDClientErrorHandler errorHandler) {
    if((prefix != null) && (!prefix.isEmpty())) {
        this.prefix = String.format("%s.", prefix);
    } else {
        this.prefix = "";
    }

    if(errorHandler == null) {
      handler = NO_OP_HANDLER;
    }
    else {
      handler = errorHandler;
    }

    /* Empty list should be null for faster comparison */
    if((constantTags != null) && (constantTags.length == 0)) {
      constantTags = null;
    }

    if(constantTags != null) {
      constantTagsRendered = tagString(constantTags, null);
    } else {
      constantTagsRendered = null;
    }

    try {
        clientChannel = DatagramChannel.open();
    } catch (final Exception e) {
        throw new StatsDClientException("Failed to start StatsD client", e);
    }
  }

  /**
   * Generate a suffix conveying the given tag list to the client
   */
  static String tagString(final String[] tags, final String tagPrefix) {
      final StringBuilder sb;
      if(tagPrefix != null) {
          if((tags == null) || (tags.length == 0)) {
              return tagPrefix;
          }
          sb = new StringBuilder(tagPrefix);
          sb.append(",");
      } else {
          if((tags == null) || (tags.length == 0)) {
              return "";
          }
          sb = new StringBuilder("|#");
      }

      for(int n=tags.length - 1; n>=0; n--) {
          sb.append(tags[n]);
          if(n > 0) {
              sb.append(",");
          }
      }
      return sb.toString();
  }

  /**
   * Create dynamic lookup for the given host name and port.
   *
   * @param hostname the host name of the targeted StatsD server
   * @param port     the port of the targeted StatsD server
   * @return a function to perform the lookup
   */
  public static Callable<InetSocketAddress> volatileAddressResolution(final String hostname, final int port) {
      return new Callable<InetSocketAddress>() {
          @Override public InetSocketAddress call() throws UnknownHostException {
              return new InetSocketAddress(InetAddress.getByName(hostname), port);
          }
      };
  }

  /**
 * Lookup the address for the given host name and cache the result.
 *
 * @param hostname the host name of the targeted StatsD server
 * @param port     the port of the targeted StatsD server
 * @return a function that cached the result of the lookup
 * @throws Exception if the lookup fails, i.e. {@link UnknownHostException}
 */
  public static Callable<InetSocketAddress> staticAddressResolution(final String hostname, final int port) throws Exception {
      final InetSocketAddress address = volatileAddressResolution(hostname, port).call();
      return new Callable<InetSocketAddress>() {
          @Override public InetSocketAddress call() {
              return address;
          }
      };
  }

  protected static Callable<InetSocketAddress> staticStatsDAddressResolution(final String hostname,
      final int port) throws StatsDClientException {
      try {
          return staticAddressResolution(hostname, port);
      } catch (final Exception e) {
          throw new StatsDClientException("Failed to lookup StatsD host", e);
      }
  }

  /**
   * Generate a suffix conveying the given tag list to the client
   */
  String tagString(final String[] tags) {
      return tagString(tags, constantTagsRendered);
  }

  /**
   * Adjusts the specified counter by a given delta.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the counter to adjust
   * @param delta
   *     the amount to adjust the counter by
   * @param tags
   *     array of tags to be added to the data
   */
  public void count(final String aspect, final long delta, final String... tags) {
      send(String.format("%s%s:%d|c%s", prefix, aspect, delta, tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  public void count(final String aspect, final long delta, final double sampleRate, final String...tags) {
    if(isInvalidSample(sampleRate)) {
      return;
    }
    send(String.format("%s%s:%d|c|@%f%s", prefix, aspect, delta, sampleRate, tagString(tags)));
  }

  /**
   * Adjusts the specified counter by a given delta.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the counter to adjust
   * @param delta
   *     the amount to adjust the counter by
   * @param tags
   *     array of tags to be added to the data
   */
  public void count(final String aspect, final double delta, final String... tags) {
      send(String.format("%s%s:%s|c%s", prefix, aspect, NUMBER_FORMATTERS.get().format(delta), tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  public void count(final String aspect, final double delta, final double sampleRate, final String...tags) {
      if(isInvalidSample(sampleRate)) {
          return;
      }
      send(String.format("%s%s:%s|c|@%f%s", prefix, aspect, NUMBER_FORMATTERS.get().format(delta), sampleRate, tagString(tags)));
  }

  /**
   * Increments the specified counter by one.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the counter to increment
   * @param tags
   *     array of tags to be added to the data
   */
  public void incrementCounter(final String aspect, final String... tags) {
      count(aspect, 1, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void incrementCounter(final String aspect, final double sampleRate, final String... tags) {

    count(aspect, 1, sampleRate, tags);
  }

  /**
   * Convenience method equivalent to {@link #incrementCounter(String, String[])}.
   */
  public void increment(final String aspect, final String... tags) {
      incrementCounter(aspect, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void increment(final String aspect, final double sampleRate, final String...tags ) {
    incrementCounter(aspect, sampleRate, tags);
  }

  /**
   * Decrements the specified counter by one.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the counter to decrement
   * @param tags
   *     array of tags to be added to the data
   */
  public void decrementCounter(final String aspect, final String... tags) {
      count(aspect, -1, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void decrementCounter(String aspect, final double sampleRate, final String... tags) {
      count(aspect, -1, sampleRate, tags);
  }

  /**
   * Convenience method equivalent to {@link #decrementCounter(String, String[])}.
   */
  public void decrement(final String aspect, final String... tags) {
      decrementCounter(aspect, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void decrement(final String aspect, final double sampleRate, final String... tags) {
      decrementCounter(aspect, sampleRate, tags);
  }

  /**
   * Records the latest fixed value for the specified named gauge.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the gauge
   * @param value
   *     the new reading of the gauge
   * @param tags
   *     array of tags to be added to the data
   */
  public void recordGaugeValue(final String aspect, final double value, final String... tags) {
      /* Intentionally using %s rather than %f here to avoid
       * padding with extra 0s to represent precision */
      send(String.format("%s%s:%s|g%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  public void recordGaugeValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    if(isInvalidSample(sampleRate)) {
      return;
    }
      send(String.format("%s%s:%s|g|@%f%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), sampleRate, tagString(tags)));
  }

  /**
   * Convenience method equivalent to {@link #recordGaugeValue(String, double, String[])}.
   */
  public void gauge(final String aspect, final double value, final String... tags) {
      recordGaugeValue(aspect, value, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void gauge(final String aspect, final double value, final double sampleRate, final String... tags) {
      recordGaugeValue(aspect, value, sampleRate, tags);
  }

  /**
   * Records the latest fixed value for the specified named gauge.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the gauge
   * @param value
   *     the new reading of the gauge
   * @param tags
   *     array of tags to be added to the data
   */
  public void recordGaugeValue(final String aspect, final long value, final String... tags) {
      send(String.format("%s%s:%d|g%s", prefix, aspect, value, tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  public void recordGaugeValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    if(isInvalidSample(sampleRate)) {
      return;
    }
      send(String.format("%s%s:%d|g|@%f%s", prefix, aspect, value, sampleRate, tagString(tags)));
  }

  /**
   * Convenience method equivalent to {@link #recordGaugeValue(String, long, String[])}.
   */
  public void gauge(final String aspect, final long value, final String... tags) {
      recordGaugeValue(aspect, value, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void gauge(final String aspect, final long value, final double sampleRate, final String... tags) {
      recordGaugeValue(aspect, value, sampleRate, tags);
  }

  /**
   * Records an execution time in milliseconds for the specified named operation.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the timed operation
   * @param timeInMs
   *     the time in milliseconds
   * @param tags
   *     array of tags to be added to the data
   */
  public void recordExecutionTime(final String aspect, final long timeInMs, final String... tags) {
      send(String.format("%s%s:%d|ms%s", prefix, aspect, timeInMs, tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  public void recordExecutionTime(final String aspect, final long timeInMs, final double sampleRate, final String... tags) {
    if(isInvalidSample(sampleRate)) {
      return;
    }
      send(String.format("%s%s:%d|ms|@%f%s", prefix, aspect, timeInMs, sampleRate, tagString(tags)));
  }

  /**
   * Convenience method equivalent to {@link #recordExecutionTime(String, long, String[])}.
   */
  public void time(final String aspect, final long value, final String... tags) {
      recordExecutionTime(aspect, value, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void time(final String aspect, final long value, final double sampleRate, final String... tags) {
      recordExecutionTime(aspect, value, sampleRate, tags);
  }

  /**
   * Records a value for the specified named histogram.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the histogram
   * @param value
   *     the value to be incorporated in the histogram
   * @param tags
   *     array of tags to be added to the data
   */
  public void recordHistogramValue(final String aspect, final double value, final String... tags) {
      /* Intentionally using %s rather than %f here to avoid
       * padding with extra 0s to represent precision */
      send(String.format("%s%s:%s|h%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  public void recordHistogramValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    if(isInvalidSample(sampleRate)) {
      return;
    }
      /* Intentionally using %s rather than %f here to avoid
       * padding with extra 0s to represent precision */
    send(String.format("%s%s:%s|h|@%f%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), sampleRate, tagString(tags)));
  }

  /**
   * Convenience method equivalent to {@link #recordHistogramValue(String, double, String[])}.
   */
  public void histogram(final String aspect, final double value, final String... tags) {
      recordHistogramValue(aspect, value, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void histogram(final String aspect, final double value, final double sampleRate, final String... tags) {
      recordHistogramValue(aspect, value, sampleRate, tags);
  }

  /**
   * Records a value for the specified named histogram.
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the histogram
   * @param value
   *     the value to be incorporated in the histogram
   * @param tags
   *     array of tags to be added to the data
   */
  public void recordHistogramValue(final String aspect, final long value, final String... tags) {
      send(String.format("%s%s:%d|h%s", prefix, aspect, value, tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  public void recordHistogramValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    if(isInvalidSample(sampleRate)) {
      return;
    }
      send(String.format("%s%s:%d|h|@%f%s", prefix, aspect, value, sampleRate, tagString(tags)));
  }

  /**
   * Convenience method equivalent to {@link #recordHistogramValue(String, long, String[])}.
   */
  public void histogram(final String aspect, final long value, final String... tags) {
      recordHistogramValue(aspect, value, tags);
  }

  /**
   * {@inheritDoc}
   */
  public void histogram(final String aspect, final long value, final double sampleRate, final String... tags) {
      recordHistogramValue(aspect, value, sampleRate, tags);
  }

  /**
   * Records a value for the specified named distribution.
   * <p>
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the distribution
   * @param value
   *     the value to be incorporated in the distribution
   * @param tags
   *     array of tags to be added to the data
   */
  @Override
  public void recordDistributionValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
    send(String.format("%s%s:%s|d%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void recordDistributionValue(final String aspect, final double value, final double sampleRate,
      final String... tags) {
    if (isInvalidSample(sampleRate)) {
      return;
    }
        /* Intentionally using %s rather than %f here to avoid
    	   * padding with extra 0s to represent precision */
    send(String.format("%s%s:%s|d|@%f%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), sampleRate,
        tagString(tags)));
  }

  /**
   * Convenience method equivalent to {@link #recordDistributionValue(String, double, String[])}.
   */
  @Override
  public void distribution(final String aspect, final double value, final String... tags) {
    recordDistributionValue(aspect, value, tags);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void distribution(final String aspect, final double value, final double sampleRate, final String... tags) {
    recordDistributionValue(aspect, value, sampleRate, tags);
  }

  /**
   * Records a value for the specified named distribution.
   * <p>
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the distribution
   * @param value
   *     the value to be incorporated in the distribution
   * @param tags
   *     array of tags to be added to the data
   */
  @Override
  public void recordDistributionValue(final String aspect, final long value, final String... tags) {
    send(String.format("%s%s:%d|d%s", prefix, aspect, value, tagString(tags)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void recordDistributionValue(final String aspect, final long value, final double sampleRate,
      final String... tags) {
    if (isInvalidSample(sampleRate)) {
      return;
    }
    send(String.format("%s%s:%d|d|@%f%s", prefix, aspect, value, sampleRate, tagString(tags)));
  }

  /**
   * Convenience method equivalent to {@link #recordDistributionValue(String, long, String[])}.
   */
  @Override
  public void distribution(final String aspect, final long value, final String... tags) {
    recordDistributionValue(aspect, value, tags);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void distribution(final String aspect, final long value, final double sampleRate, final String... tags) {
    recordDistributionValue(aspect, value, sampleRate, tags);
  }

    private String eventMap(final Event event) {
      final StringBuilder res = new StringBuilder("");

      final long millisSinceEpoch = event.getMillisSinceEpoch();
      if (millisSinceEpoch != -1) {
          res.append("|d:").append(millisSinceEpoch / 1000);
      }

      final String hostname = event.getHostname();
      if (hostname != null) {
          res.append("|h:").append(hostname);
      }

      final String aggregationKey = event.getAggregationKey();
      if (aggregationKey != null) {
          res.append("|k:").append(aggregationKey);
      }

      final String priority = event.getPriority();
      if (priority != null) {
          res.append("|p:").append(priority);
      }

      final String alertType = event.getAlertType();
      if (alertType != null) {
          res.append("|t:").append(alertType);
      }

      return res.toString();
  }

  /**
   * Records an event
   *
   * <p>This method is a DataDog extension, and may not work with other servers.</p>
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param event
   *     The event to record
   * @param tags
   *     array of tags to be added to the data
   *
   * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events-1">http://docs.datadoghq.com/guides/dogstatsd/#events-1</a>
   */
  public void recordEvent(final Event event, final String... tags) {
      final String title = escapeEventString(prefix + event.getTitle());
      final String text = escapeEventString(event.getText());
      send(String.format("_e{%d,%d}:%s|%s%s%s",
              title.length(), text.length(), title, text, eventMap(event), tagString(tags)));
  }

  private String escapeEventString(final String title) {
      return title.replace("\n", "\\n");
  }

  /**
   * Records a run status for the specified named service check.
   *
   * <p>This method is a DataDog extension, and may not work with other servers.</p>
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param sc
   *     the service check object
   */
  public void recordServiceCheckRun(final ServiceCheck sc) {
      send(toStatsDString(sc));
  }

  private String toStatsDString(final ServiceCheck sc) {
      // see http://docs.datadoghq.com/guides/dogstatsd/#service-checks
      final StringBuilder sb = new StringBuilder();
      sb.append(String.format("_sc|%s|%d", sc.getName(), sc.getStatus()));
      if (sc.getTimestamp() > 0) {
          sb.append(String.format("|d:%d", sc.getTimestamp()));
      }
      if (sc.getHostname() != null) {
          sb.append(String.format("|h:%s", sc.getHostname()));
      }
      sb.append(tagString(sc.getTags()));
      if (sc.getMessage() != null) {
          sb.append(String.format("|m:%s", sc.getEscapedMessage()));
      }
      return sb.toString();
  }

  /**
   * Convenience method equivalent to {@link #recordServiceCheckRun(ServiceCheck sc)}.
   */
  public void serviceCheck(final ServiceCheck sc) {
      recordServiceCheckRun(sc);
  }

  /**
   * Records a value for the specified set.
   *
   * Sets are used to count the number of unique elements in a group. If you want to track the number of
   * unique visitor to your site, sets are a great way to do that.
   *
   * <p>This method is a DataDog extension, and may not work with other servers.</p>
   *
   * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
   *
   * @param aspect
   *     the name of the set
   * @param value
   *     the value to track
   * @param tags
   *     array of tags to be added to the data
   *
   * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq.com/guides/dogstatsd/#sets</a>
   */
  public void recordSetValue(final String aspect, final String value, final String... tags) {
      // documentation is light, but looking at dogstatsd source, we can send string values
      // here instead of numbers
      send(String.format("%s%s:%s|s%s", prefix, aspect, value, tagString(tags)));
  }

  @Override
  public void close() {
      stop();
  }

  protected abstract void send(String message);

  private boolean isInvalidSample(double sampleRate) {
      return sampleRate != 1 && ThreadLocalRandom.current().nextDouble() > sampleRate;
  }

  @Override
  public void stop() {
    if (clientChannel != null) {
      try {
        clientChannel.close();
      }
      catch (final IOException e) {
        handler.handle(e);
      }
    }
  }

  protected class Sender {
      private final ByteBuffer sendBuffer = ByteBuffer.allocate(PACKET_SIZE_BYTES);
      private final Callable<InetSocketAddress> addressLookup;

      public Sender(Callable<InetSocketAddress> addressLookup) {
          this.addressLookup = addressLookup;
      }

      protected void addToBuffer(String message) throws Exception {
          final byte[] data = message.getBytes(MESSAGE_CHARSET);
          if (sendBuffer.remaining() < (data.length + 1)) {
              blockingSend();
          }
          if (sendBuffer.position() > 0) {
              sendBuffer.put((byte) '\n');
          }
          sendBuffer.put(data);
      }

      protected void blockingSend() throws Exception {
          final InetSocketAddress address = addressLookup.call();
          final int sizeOfBuffer = sendBuffer.position();
          sendBuffer.flip();

          final int sentBytes = clientChannel.send(sendBuffer, address);
          sendBuffer.limit(sendBuffer.capacity());
          sendBuffer.rewind();

          if (sizeOfBuffer != sentBytes) {
              handler.handle(
                  new IOException(
                      String.format(
                          "Could not send entirely stat %s to host %s:%d. Only sent %d bytes "
                              + "out of %d bytes",
                          sendBuffer.toString(),
                          address.getHostName(),
                          address.getPort(),
                          sentBytes,
                          sizeOfBuffer)));
          }
      }
  }
}
