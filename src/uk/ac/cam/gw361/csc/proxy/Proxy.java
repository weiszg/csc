package uk.ac.cam.gw361.csc.proxy;

import sun.nio.ch.Net;
import uk.ac.cam.gw361.csc.dht.PeerManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by gellert on 21/02/2016.
 */
public class Proxy {
    private static boolean debug = false;
    int localPort;
    String targetHost;
    int targetPort;
    long bytesPerSec;
    int latency;
    boolean closed = false;

    ProxyListener listener;
    ProxyRec outRec, inRec;
    ProxySend outSend, inSend;
    ServerSocket ssocket;

    static final int maxBufferSize = 100;

    public Proxy(int localPort, String targetHost, int targetPort,
                 int latency, long bandwidth) {
        this.localPort = localPort;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.latency = latency;
        this.bytesPerSec = bandwidth;

        try {
            ssocket = new ServerSocket(localPort);
        } catch (IOException e) {
            System.out.println("Proxy error at Proxy: " + e.toString());
            return;
        }

        listener = new ProxyListener(this, ssocket);
        outRec = new ProxyRec(this, false);
        inRec = new ProxyRec(this, true);
        outSend = new ProxySend(this, inRec, false);
        inSend = new ProxySend(this, outRec, true);

        listener.start();
        if (debug) {
            ProxyLogger logger = new ProxyLogger(this);
            logger.start();
        }
    }

    synchronized void close() {
        closed = true;
        inSend.close();
        outSend.close();
        listener.close();
    }

    public long getOutSpeed() {
        return outSend.getBytesLastSec();
    }

    public long getInSpeed() {
        return inSend.getBytesLastSec();
    }

    public synchronized boolean isAlive() {
        return !closed;
    }

    public synchronized int getLocalPort() {
        return ssocket.getLocalPort();
    }
}

class ForwardPacket {
    long receivedTime;
    byte[] data;

    ForwardPacket(byte[] data) {
        this.receivedTime = System.currentTimeMillis();
        this.data = data;
    }
}

class PacketDescriptor {
    long sentTime;
    long length;

    PacketDescriptor(long length) {
        this.sentTime = System.currentTimeMillis();
        this.length = length;
    }
}

class ProxySend extends Thread {
    private static boolean debug = false;
    boolean in;
    Proxy proxy;
    ProxyRec tie;
    OutputStream socketOut;
    long latencyDeviation;
    private boolean closed = false;
    private LinkedList<PacketDescriptor> history = new LinkedList<>();
    private long bytesLastSec = 0;

    ProxySend(Proxy proxy, ProxyRec tie, boolean in) {
        this.proxy = proxy;
        this.tie = tie;
        this.in = in;
        latencyDeviation = 0;
    }

    synchronized long getBytesLastSec() {
        long time = System.currentTimeMillis();
        while (!history.isEmpty() && (history.get(0).sentTime < time-1000)) {
            bytesLastSec -= history.get(0).length;
            history.remove(0);
        }
        return bytesLastSec;
    }

    private synchronized void reportBytesSent(long length) {
        PacketDescriptor p = new PacketDescriptor(length);
        history.add(p);
        bytesLastSec += length;
        PeerManager.reportBytesSent(in, length, false);
    }

    private void pause(long millis) {
        // pause execution accounting for deviations in sleep time
        if (debug) System.out.println("Pause  " + millis + ", deviation: " + latencyDeviation);
        if (millis + latencyDeviation < 0) {
            latencyDeviation += millis;
            return;
        }

        long oldTime = System.nanoTime();
        LockSupport.parkNanos((millis + latencyDeviation) * 1000000);
        long newTime = System.nanoTime();

        // update deviation measure
        latencyDeviation = (millis + latencyDeviation) - ((newTime - oldTime) / 1000000);
    }

    private void forwardData() throws IOException {
        while (true) {
            synchronized (this) {
                if (closed)
                    return;
            }

            ForwardPacket top = tie.getFromBuffer();
            if (top != null) {
                // calculate how much more we need to wait
                long time = System.currentTimeMillis();
                long toWait = proxy.latency - (time - top.receivedTime);

                if (toWait <= 0) {
                    latencyDeviation += toWait;
                    // send data if bandwidth enough
                    if (getBytesLastSec() < proxy.bytesPerSec) {
                        reportBytesSent(top.data.length);
                        if (debug)
                            System.out.println("Sending " + top.data.length + " bytes, dir: " +
                                (in ? "incoming" : "outgoing"));

                        synchronized (this) {
                            socketOut.write(top.data);
                        }
                        tie.popFromBuffer();
                    } else {
                        pause(100);
                    }
                } else {
                    pause(toWait);
                }
            } else {
                pause(Math.max(proxy.latency, 100));
            }
        }
    }

    public void run() {
        try {
            forwardData();
        } catch (IOException e) {
            System.out.println("Proxy error at ProxySend: " + e.toString());
            proxy.close();
        }
    }

    public synchronized void close() {
        try {
            closed = true;
            socketOut.flush();
        } catch (IOException e) { }
    }
}

class ProxyRec extends Thread {
    private static boolean debug = false;
    private static boolean verboseDebug = false;
    InputStream socketIn;
    Proxy proxy;
    boolean in;

    List<ForwardPacket> buffer = new LinkedList<>();

    ProxyRec(Proxy proxy, boolean in) {
        this.proxy = proxy;
        this.in = in;
    }

    synchronized boolean addToBuffer(ForwardPacket p) {
        if (buffer.size() >= Proxy.maxBufferSize)
            return false;

        buffer.add(p);
        return true;
    }

    synchronized ForwardPacket getFromBuffer() {
        if (!buffer.isEmpty())
            return buffer.get(0);
        else
            return null;
    }

    synchronized boolean popFromBuffer() {
        if (buffer.isEmpty())
            return false;

        buffer.remove(0);
        return true;
    }

    private void printData(byte[] data) {
        try {
            String str = new String(data, "UTF-8");
            System.out.println(str);
        } catch (UnsupportedEncodingException e) {}
    }

    private void acceptData() throws IOException {
        int bytesRead;
        byte[] data = new byte[1024];

        while ((bytesRead = socketIn.read(data, 0, data.length)) != -1) {
            byte[] choppedData = Arrays.copyOf(data, bytesRead);

            if (debug)
                System.out.println("Received " + bytesRead + " bytes, dir: " +
                    (in ? "incoming" : "outgoing"));
            if (verboseDebug)
                printData(choppedData);

            ForwardPacket p = new ForwardPacket(choppedData);
            while (!addToBuffer(p)) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    public void run() {
        try {
            acceptData();
            System.out.println("Data transfer finished");
        } catch (IOException e) {
            System.out.println("Proxy error at ProxyRec: " + e.toString());
        } finally {
            proxy.close();
        }
    }
}

class ProxyListener extends Thread {
    ServerSocket listener;
    Proxy proxy;
    Socket socketRec, socketSend;

    ProxyListener(Proxy proxy, ServerSocket ssocket) {
        this.proxy = proxy;
        this.listener = ssocket;
    }

    public void run() {
        try {
            listener.setSoTimeout(0);
            socketRec = listener.accept();
            listener.close();

            socketSend = new Socket();
            socketSend.setSoTimeout(0);
            socketSend.connect(new InetSocketAddress(proxy.targetHost, proxy.targetPort), 900);

            proxy.inRec.socketIn = socketRec.getInputStream();
            proxy.inSend.socketOut = socketRec.getOutputStream();
            proxy.outRec.socketIn = socketSend.getInputStream();
            proxy.outSend.socketOut = socketSend.getOutputStream();

            // start threads
            proxy.inRec.start();
            proxy.inSend.start();
            proxy.outRec.start();
            proxy.outSend.start();
        } catch (IOException e) {
            System.out.println("Proxy error at ProxyListener: " + e.toString());
        }
    }

    void close() {
        try { socketRec.close(); } catch (IOException e) { }
        try { socketSend.close(); } catch (IOException e) { }
    }
}

class ProxyLogger extends Thread {
    Proxy proxy;
    ProxyLogger(Proxy proxy) {
        this.proxy = proxy;
    }

    public void run() {
        while (proxy.isAlive()) {
            System.out.println("Rx: " + proxy.getInSpeed() + ", Tx: " + proxy.getOutSpeed() +
                    ", on:" + proxy.isAlive());

            try { Thread.sleep(1000); } catch (InterruptedException e) { }
        }
        System.out.println("Proxy stopped");
    }
}
