package uk.ac.cam.gw361.csc;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.Scanner;

/**
 * Created by gellert on 01/11/2015.
 */
public class Main {
    public static void main(String[] args) {
        System.setProperty("sun.rmi.transport.proxy.connectTimeout", "10");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "10");
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "10");


        String userName = (args.length < 1) ? null : args[0];
        String host = (args.length < 2) ? null : args[1];
        String ct = (args.length < 3) ? null : args[2];
        int count = 1;

        if (userName == null) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("username");
            userName = scanner.next();
            host = scanner.next();
            if (host.equals("-")) host = null;
            ct = scanner.next();
            if (ct.equals("-")) ct = null;
        }

        if (ct != null)
            count = Integer.parseInt(ct);

        LocalPeer localPeer = new LocalPeer(userName);
        if (host != null) {
            localPeer.join(host);
        }

        LocalPeer[] extraPeer = count>1 ? new LocalPeer[count-1] : null;
        for (int i=1; i<count; i++) {
            //try { Thread.sleep(1000); } catch (InterruptedException ie) {}
            String un = userName.split(":")[0];
            extraPeer[i-1] = new LocalPeer(un + "-" + i + ":" + (8000 + i));
            if (host != null) {
                extraPeer[i-1].join("localhost:" + localPeer.localAddress.getPort());
            }
        }

        Thread commandReader = new CommandReader(localPeer);
        commandReader.start();

    }

    public static void debug1() {
        LocalPeer p1 = new LocalPeer("1:8001");
        LocalPeer p2 = new LocalPeer("2:8002");
        p2.join("localhost:8001");
        try { Thread.sleep(100); } catch (InterruptedException e) { }
        p2.disconnect();
        p2 = new LocalPeer("2:8002");
        p2.join("localhost:8001");

        System.out.println("done");
    }
}


class CommandReader extends Thread {
    private LocalPeer localPeer;

    public CommandReader(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String readStr = scanner.nextLine();
            System.out.println("processing command " + readStr);
            try {
                if (readStr.equals("nb")) {
                    localPeer.getNeighbourState().print("");
                } else if (readStr.equals("stabilise")) {
                    localPeer.stabilise();
                } else if (readStr.startsWith("dl")) {
                    readStr = readStr.substring("dl ".length());
                    BigInteger target = new BigInteger(readStr);
                    System.out.println("downloading " + target.toString());
                    localPeer.getFile(target);
                } else if (readStr.startsWith("ul")) {
                    readStr = readStr.substring("ul ".length());
                    localPeer.publishFile(readStr);
                    System.out.println("upload started");
                } else System.out.println("Unrecognised command: " + readStr);
            } catch (IOException | NumberFormatException e) {
                e.printStackTrace();
            }
        }
    }
}
