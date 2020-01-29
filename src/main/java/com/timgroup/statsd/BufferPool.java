package com.timgroup.statsd;

import java.nio.ByteBuffer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BufferPool {
    private final BlockingQueue<ByteBuffer> pool;
    private final int size;


    BufferPool(final int poolSize, int bufferSize, final boolean direct) throws InterruptedException {

        size = poolSize;
        pool = new ArrayBlockingQueue<ByteBuffer>(poolSize);
        for (int i = 0; i < size ; i++) {
            if (direct) {
                pool.put(ByteBuffer.allocateDirect(bufferSize));
            } else {
                pool.put(ByteBuffer.allocate(bufferSize));
            }
        }
    }

    ByteBuffer borrow() throws InterruptedException {
        return pool.take();
    }

    void put(ByteBuffer buffer) throws InterruptedException {
        pool.put(buffer);
    }

    int available() {
        return pool.size();
    }
}
