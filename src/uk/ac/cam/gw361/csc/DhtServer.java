package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Created by gellert on 01/11/2015.
 */
public class DhtServer implements DhtComm {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private Map<DhtPeerAddress, Socket> uploads = new HashMap<>();

    public DhtServer(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void startServer(int localPort) {
        try {
            DhtComm stub = (DhtComm) UnicastRemoteObject.exportObject(this, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.createRegistry(localPort);
            registry.bind("DhtComm", stub);

            if (debug) System.out.println("DHT Server ready on " + localPort);
        } catch (Exception e) {
            System.err.println("DHT Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

    private void acceptConnection(DhtPeerAddress source) {
        try {
            String clientHost = RemoteServer.getClientHost();
            localPeer.getNeighbourState().addNeighbour(
                    new DhtPeerAddress(source.getUserID(), clientHost, source.getPort()));
        } catch (ServerNotActiveException e) {
            e.printStackTrace();
        }
    }

    public DhtPeerAddress lookup(DhtPeerAddress source, BigInteger target) {
        if (debug) System.out.println("server lookup");
        acceptConnection(source);
        return localPeer.getClient().lookup(target);
    }

    public NeighbourState getNeighbourState(DhtPeerAddress source) {
        if (debug) System.out.println("server getneighbourstate");
        acceptConnection(source);
        return localPeer.getNeighbourState();
    }

    public Long download(DhtPeerAddress source, Integer port, BigInteger target) {

        return null;
    }

    public Map<BigInteger, Long> getRange(DhtPeerAddress source,
                                             BigInteger from, BigInteger to) {
        return null;
    }
}


class Uploader extends Thread {
    Socket socket;
    FileInputStream fileInputStream;

    public Uploader(Socket socket, FileInputStream fileInputStream) {
        this.socket = socket;
        this.fileInputStream = fileInputStream;
    }

    public void run() {
        byte[] data = new byte[100000];
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        try {
            OutputStream outputStream = socket.getOutputStream();
            System.out.println("Sending Files...");

            int bytesRead = 0;
            while (bytesRead != -1) {
                bytesRead = bufferedInputStream.read(data, 0, data.length);
                outputStream.write(data, 0, bytesRead);
            }
            outputStream.flush();
            socket.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
