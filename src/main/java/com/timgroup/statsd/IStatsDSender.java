package com.timgroup.statsd;

public interface IStatsDSender extends Runnable {
    void shutdown();
    boolean send(String s);
}