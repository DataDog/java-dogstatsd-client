package com.timgroup.statsd;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class BufferPool {
    private final Queue<ByteBuffer> pool;
    private final int size;


    BufferPool(final int poolSize, int bufferSize, final boolean direct) {

        size = poolSize;
        pool = new ArrayBlockingQueue<ByteBuffer>(poolSize);
        for (int i=0; i<size ; i++) {
            if (direct) {
                pool.offer(ByteBuffer.allocateDirect(bufferSize));
            } else {
                pool.offer(ByteBuffer.allocate(bufferSize));
            }
        }
    }

    ByteBuffer borrow() {
        return pool.poll();
    }

    void put(ByteBuffer buffer) {
        pool.offer(buffer);
    }

    int available() {
        return pool.size();
    }
}
