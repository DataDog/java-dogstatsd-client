package com.timgroup.statsd;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class NamedPipeClientChannelTest {

    @Test
    public void write_after_close_throws_without_opening_the_pipe() throws Exception {
        // The pipe doesn't exist, so a write that tried to open it would throw
        // FileNotFoundException instead.
        final NamedPipeClientChannel channel =
                new NamedPipeClientChannel(new NamedPipeSocketAddress("missing-dir/pipe"));
        assertTrue(channel.isOpen());

        channel.close();

        assertFalse(channel.isOpen());
        assertThrows(
                ClosedChannelException.class,
                new ThrowingRunnable() {
                    @Override
                    public void run() throws Exception {
                        channel.write(ByteBuffer.wrap(new byte[] {1}));
                    }
                });
    }
}
