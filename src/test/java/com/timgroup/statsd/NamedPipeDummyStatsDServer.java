package com.timgroup.statsd;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinDef.DWORDByReference;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.ptr.IntByReference;
import java.io.IOException;
import java.nio.ByteBuffer;

// Template from https://github.com/java-native-access/jna/blob/master/contrib/platform/test/com/sun/jna/platform/win32/Kernel32NamedPipeTest.java
// And https://docs.microsoft.com/en-us/windows/win32/ipc/multithreaded-pipe-server
public class NamedPipeDummyStatsDServer extends DummyStatsDServer {
    private final HANDLE hNamedPipe;
    private volatile boolean clientConnected = false;
    private volatile boolean isOpen = true;

    public NamedPipeDummyStatsDServer(String pipeName) {
        String normalizedPipeName = NamedPipeSocketAddress.normalizePipeName(pipeName);

        hNamedPipe= Kernel32.INSTANCE.CreateNamedPipe(normalizedPipeName,
                WinBase.PIPE_ACCESS_DUPLEX,        // dwOpenMode
                WinBase.PIPE_TYPE_BYTE | WinBase.PIPE_READMODE_BYTE | WinBase.PIPE_WAIT,    // dwPipeMode
                1,    // nMaxInstances,
                Byte.MAX_VALUE,    // nOutBufferSize,
                Byte.MAX_VALUE,    // nInBufferSize,
                1000,    // nDefaultTimeOut,
                null);    // lpSecurityAttributes

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
        if (!clientConnected) {
            boolean connected = Kernel32.INSTANCE.ConnectNamedPipe(hNamedPipe, null);
            if (connected) {
                clientConnected = true;
            } else {
                close();
                return;
            }
        }

        IntByReference bytesRead = new IntByReference();
        boolean success = Kernel32.INSTANCE.ReadFile(
                hNamedPipe,        // handle to pipe
                packet.array(),    // buffer to receive data
                packet.remaining(), // size of buffer
                bytesRead, // number of bytes read
                null);        // not overlapped I/O
        packet.position(bytesRead.getValue());
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        Kernel32.INSTANCE.CloseHandle(hNamedPipe);
    }
}
