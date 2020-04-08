package com.timgroup.statsd;

import java.nio.ByteBuffer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BufferPool {
    private final BlockingQueue<ByteBuffer> pool;
    private final int size;
    private final int bufferSize;
    private final boolean direct;


    BufferPool(final int poolSize, int bufferSize, final boolean direct) throws InterruptedException {

        size = poolSize;
        this.bufferSize = bufferSize;
        this.direct = direct;

        pool = new ArrayBlockingQueue<ByteBuffer>(poolSize);
        for (int i = 0; i < size ; i++) {
            if (direct) {
                pool.put(ByteBuffer.allocateDirect(bufferSize));
            } else {
                pool.put(ByteBuffer.allocate(bufferSize));
            }
        }
    }

    BufferPool(final BufferPool pool) throws InterruptedException {
        this.size = pool.size;
        this.bufferSize = pool.bufferSize;
        this.direct = pool.direct;
        this.pool = new ArrayBlockingQueue<ByteBuffer>(pool.size);
        for (int i = 0; i < size ; i++) {
            if (direct) {
                this.pool.put(ByteBuffer.allocateDirect(bufferSize));
            } else {
                this.pool.put(ByteBuffer.allocate(bufferSize));
            }
        }
    }

    ByteBuffer borrow() throws InterruptedException {
        return pool.take();
    }

    void put(ByteBuffer buffer) throws InterruptedException {
        pool.put(buffer);
    }

    int getSize() {
        return size;
    }

    int available() {
        return pool.size();
    }
}
