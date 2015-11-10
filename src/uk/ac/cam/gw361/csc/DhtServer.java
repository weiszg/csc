package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;


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

    public DhtPeerAddress lookup(DhtPeerAddress source, BigInteger target) throws IOException {
        if (debug) System.out.println("server lookup");
        acceptConnection(source);
        return localPeer.getClient().lookup(target);
    }

    public NeighbourState getNeighbourState(DhtPeerAddress source) {
        if (debug) System.out.println("server getneighbourstate");
        acceptConnection(source);
        return localPeer.getNeighbourState();
    }

    public Long upload(DhtPeerAddress source, Integer port, BigInteger target) throws IOException {
        acceptConnection(source);
        Long size = localPeer.getFileStore().getLength(target);
        FileInputStream fis = localPeer.getFileStore().readFile(target);

        Socket socket = new Socket(source.getHost(), port);
        Thread uploader = new FileTransfer(socket, fis);
        uploader.start();
        return size;
    }

    public void download(DhtPeerAddress source, Integer port, BigInteger target) throws IOException {
        acceptConnection(source);
        FileOutputStream fos = localPeer.getFileStore().writeFile(target);

        Socket socket = new Socket(source.getHost(), port);
        Thread downloader = new FileTransfer(socket, fos);
        downloader.start();
    }

    public Map<BigInteger, Long> getRange(DhtPeerAddress source,
                                             BigInteger from, BigInteger to) {
        return null;
    }

    public Boolean isAlive(DhtPeerAddress source) {
        acceptConnection(source);
        return true;
    }
}
