package com.timgroup.statsd;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Calendar;
import java.util.concurrent.Callable;

public class BlockingStatsDClient extends DefaultStatsDClient {

  private final Sender sender;

  public BlockingStatsDClient(String prefix, String hostname, int serverPort) {
    this(prefix, null, null, staticStatsDAddressResolution(hostname, serverPort));
  }

  public BlockingStatsDClient(String prefix, String[] constantTags,
      StatsDClientErrorHandler errorHandler, Callable<InetSocketAddress> addressLookup) {
    super(prefix, constantTags, errorHandler);
    sender = new Sender(addressLookup);
  }

  @Override
  protected void send(String message) {
    try {
      sender.addToBuffer(message);
      sender.blockingSend();
    } catch (Exception e) {
      handler.handle(e);
    }
  }

}
