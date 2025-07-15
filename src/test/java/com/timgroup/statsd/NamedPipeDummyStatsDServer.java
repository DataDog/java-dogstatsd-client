package com.timgroup.statsd;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinError;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.ptr.IntByReference;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

// Template from
// https://github.com/java-native-access/jna/blob/master/contrib/platform/test/com/sun/jna/platform/win32/Kernel32NamedPipeTest.java
// And https://docs.microsoft.com/en-us/windows/win32/ipc/multithreaded-pipe-server
public class NamedPipeDummyStatsDServer extends DummyStatsDServer {
    private static final Logger log = Logger.getLogger("NamedPipeDummyStatsDServer");
    private final HANDLE hNamedPipe;
    private volatile boolean clientConnected = false;
    private volatile boolean isOpen = true;

    public NamedPipeDummyStatsDServer(String pipeName) {
        String normalizedPipeName = NamedPipeSocketAddress.normalizePipeName(pipeName);

        hNamedPipe =
                Kernel32.INSTANCE.CreateNamedPipe(
                        normalizedPipeName,
                        WinBase.PIPE_ACCESS_DUPLEX, // dwOpenMode
                        WinBase.PIPE_TYPE_BYTE
                                | WinBase.PIPE_READMODE_BYTE
                                | WinBase.PIPE_WAIT, // dwPipeMode
                        1, // nMaxInstances,
                        Byte.MAX_VALUE, // nOutBufferSize,
                        Byte.MAX_VALUE, // nInBufferSize,
                        1000, // nDefaultTimeOut,
                        null); // lpSecurityAttributes

        if (WinBase.INVALID_HANDLE_VALUE.equals(hNamedPipe)) {
            throw new RuntimeException("Unable to create named pipe");
        }

        listen();
    }

    @Override
    protected boolean isOpen() {
        return isOpen;
    }

    @Override
    protected void receive(ByteBuffer packet) throws IOException {
        if (!isOpen) {
            throw new IOException("Server closed");
        }
        if (!clientConnected) {
            boolean connected = Kernel32.INSTANCE.ConnectNamedPipe(hNamedPipe, null);
            // ERROR_PIPE_CONNECTED means the client connected before the server
            // The connection is established
            int lastError = Kernel32.INSTANCE.GetLastError();
            connected = connected || lastError == WinError.ERROR_PIPE_CONNECTED;
            if (connected) {
                clientConnected = true;
            } else {
                log.info("Failed to connect. Last error: " + lastError);
                close();
                return;
            }
        }

        IntByReference bytesRead = new IntByReference();
        boolean success =
                Kernel32.INSTANCE.ReadFile(
                        hNamedPipe, // handle to pipe
                        packet.array(), // buffer to receive data
                        packet.remaining(), // size of buffer
                        bytesRead, // number of bytes read
                        null); // not overlapped I/O

        log.info("Read bytes. Result: " + success + ". Bytes read: " + bytesRead.getValue());
        packet.position(bytesRead.getValue());
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        Kernel32.INSTANCE.CloseHandle(hNamedPipe);
    }
}
