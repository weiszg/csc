package uk.ac.cam.gw361.csc;

import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.dht.PeerManager;
import uk.ac.cam.gw361.csc.dht.ProxiedConnector;
import uk.ac.cam.gw361.csc.dht.TimedRMISocketFactory;
import uk.ac.cam.gw361.csc.transfer.DirectTransfer;

import java.io.IOException;
import java.rmi.server.RMISocketFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Created by gellert on 01/11/2015.
 */
public class Server {
    public static LocalPeer localPeer;
    public static LocalPeer[] extraPeer;

    public static void main(String[] args) {
        // set RMI timeout properties
        System.setProperty("sun.rmi.transport.proxy.connectTimeout", "1000");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "1000");
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "1000");

        // set key/truststore
        setKeyStore();
        setTrustStore();
        // System.setProperty("javax.net.debug", "all");

        manualStart(args);
    }

    public static void setKeyStore() {
        System.setProperty("javax.net.ssl.keyStore", "keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
    }

    public static void setTrustStore() {
        System.setProperty("javax.net.ssl.trustStore", "truststore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.rmi.ssl.client.enabledProtocols", "TLSv1.2");
    }

    static void manualStart(String[] args) {
        try {
            RMISocketFactory.setSocketFactory(new TimedRMISocketFactory());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        String userName = null, host = null;
        int count = 1, stabiliseInterval = 5000;
        boolean proxied = false; int proxyBytesPerSec = 100000; int proxyLatency = 0;
        boolean cscOnly = false, freshStart = false;

        for (String arg : args) {
            if (arg.startsWith("username="))
                userName = arg.substring("username=".length());
            else if (arg.startsWith("host="))
                host = arg.substring("host=".length());
            else if (arg.startsWith("ct="))
                count = Integer.parseInt(arg.substring("ct=".length()));
            else if (arg.startsWith("stabiliseInterval="))
                stabiliseInterval = Integer.parseInt(arg.substring("stabiliseInterval=".length()));
            else if (arg.startsWith("proxied"))
                proxied = true;
            else if (arg.startsWith("proxyBytesPerSec"))
                proxyBytesPerSec = Integer.parseInt(arg.substring("proxyBytesPerSec=".length()));
            else if (arg.startsWith("proxyLatency"))
                proxyLatency = Integer.parseInt(arg.substring("proxyLatency=".length()));
            else if (arg.startsWith("intratelimit"))
                DirectTransfer.internalLimiter.ratelimit =
                        1024 * Long.parseLong(arg.substring("intratelimit=".length()));
            else if (arg.startsWith("extratelimit"))
                DirectTransfer.externalLimiter.ratelimit =
                        1024 * Long.parseLong(arg.substring("extratelimit=".length()));
            else if (arg.startsWith("count"))
                count = Integer.parseInt(arg.substring("count=".length()));
            else if (arg.startsWith("perfmon"))
                PeerManager.perfmon = true;
            else if (arg.startsWith("csconly"))
                cscOnly = true;
            else if (arg.startsWith("freshStart"))  // delete previous storage data
                freshStart = true;
            else
                System.err.println("argument couldn't be recognised: " + arg);
        }

        if (proxied) {
            PeerManager.setConnector(new ProxiedConnector(proxyLatency, proxyBytesPerSec));
            PeerManager.startLogging();
        }

        if (userName == null) {
            if (cscOnly) {
                userName = "client";
            } else {
                Scanner scanner = new Scanner(System.in);
                System.out.println("username, host");
                userName = scanner.next();
                host = scanner.next();
                if (host.equals("-")) host = null;
            }
        }

        localPeer = PeerManager.spawnPeer(userName, stabiliseInterval,
                cscOnly, freshStart);
        if (host != null) {
            localPeer.join(host);
        }

        if (count > 1) {
            System.out.println("Waiting 10s before spawning others");
            try { Thread.sleep(10000); } catch (InterruptedException e) { }
            extraPeer = new LocalPeer[count - 1];
            for (int i = 1; i < count; i++) {
                //try { Thread.sleep(1000); } catch (InterruptedException ie) {}
                String un = userName.split(":")[0];
                extraPeer[i - 1] = PeerManager.spawnPeer(un + "-" + i + ":" + (8000 + i),
                        stabiliseInterval, false, freshStart);
                extraPeer[i - 1].join(localPeer.localAddress.getConnectAddress());
            }
        }

        Thread commandReader = new CommandReader(localPeer);
        commandReader.start();
    }

    public static void debug1() {
        LocalPeer p1 = PeerManager.spawnPeer("1:8001", 1000);
        LocalPeer p2 = PeerManager.spawnPeer("2:8002", 1000);
        p2.join("localhost:8001");
        try { Thread.sleep(100); } catch (InterruptedException e) { }
        p2.disconnect();
        p2 = PeerManager.spawnPeer("2:8002", 1000);
        p2.join("localhost:8001");

        System.out.println("done");
    }
}

class CommandReader extends Thread {
    private static LocalPeer localPeer;

    public CommandReader(LocalPeer localPeer) {
        CommandReader.localPeer = localPeer;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String readStr = scanner.nextLine();
            try {
                System.out.println(localPeer.executeQuery(readStr));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
