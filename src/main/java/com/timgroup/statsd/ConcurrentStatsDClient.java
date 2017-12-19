package com.timgroup.statsd;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConcurrentStatsDClient extends BackgroundStatsDClient {

  private final ConcurrentLinkedQueue<String> queue;

  public ConcurrentStatsDClient(String prefix, String hostname, int serverPort) {
    this(prefix, null, null, staticStatsDAddressResolution(hostname, serverPort));
  }

  public ConcurrentStatsDClient(String prefix, String[] constantTags,
      StatsDClientErrorHandler errorHandler, Callable<InetSocketAddress> addressLookup) {
    super(prefix, constantTags, errorHandler);

    queue = new ConcurrentLinkedQueue<>();
    executor.submit(new QueueConsumer(addressLookup));
  }

  @Override
  protected void send(String message) {
    queue.offer(message);
  }

  private class QueueConsumer implements Runnable {
    private final Sender sender;

    QueueConsumer(final Callable<InetSocketAddress> addressLookup) {
      sender = new Sender(addressLookup);
    }

    @Override
    public void run() {
      while (!executor.isShutdown()) {
        try {
          final String message = queue.poll();
          if (message == null) {
            Thread.sleep(1000);
            continue;
          }
          sender.addToBuffer(message);
          if (null == queue.peek()) {
            sender.blockingSend();
          }
        } catch (final Exception e) {
          handler.handle(e);
        }
      }
    }
  }
}
