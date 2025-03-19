package com.timgroup.statsd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.net.SocketAddress;
import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketAddress;

import static com.timgroup.statsd.NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;

public class UnixDatagramSocketDummyStatsDServer extends DummyStatsDServer {
    private final DatagramChannel server;
    private volatile boolean serverRunning = true;

    public UnixDatagramSocketDummyStatsDServer(String socketPath) throws IOException {
        if (ClientChannelUtils.hasNativeUdsSupport()) {
            try {
                Class<?> udsAddressClass = Class.forName("java.net.UnixDomainSocketAddress");
                Object udsAddress = udsAddressClass.getMethod("of", String.class).invoke(null, socketPath);

                DatagramChannel nativeServer = DatagramChannel.open();
                nativeServer.bind((SocketAddress) udsAddress);
                this.server = nativeServer;
            } catch (ReflectiveOperationException e) {
                throw new IOException(e);
            }
        } else {
            UnixDatagramChannel jnrServer = UnixDatagramChannel.open();
            jnrServer.bind(new UnixSocketAddress(socketPath));
            this.server = jnrServer;
        }
        this.listen();
    }

    @Override
    protected boolean isOpen() {
        return serverRunning && server.isOpen();
    }

    protected void receive(ByteBuffer packet) throws IOException {
        server.receive(packet);
    }

    @Override
    public void close() throws IOException {
        serverRunning = false;
        try {
            // Give the listening thread a chance to stop
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            server.close();
        } catch (Exception e) {
            //ignore
        }
    }
}
