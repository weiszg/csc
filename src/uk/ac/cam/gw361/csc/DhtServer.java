package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.ExportException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by gellert on 01/11/2015.
 */
public class DhtServer implements DhtComm {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private Map<DhtPeerAddress, Socket> uploads = new HashMap<>();
    private final int port;
    private final Registry registry;

    public DhtServer(LocalPeer localPeer, int port) {
        this.port = port;
        this.localPeer = localPeer;
        Registry reg = null;
        try {
            try {
                reg = LocateRegistry.createRegistry(port);
            } catch (ExportException e) {
                reg = LocateRegistry.getRegistry(port);
            }
        } catch (RemoteException e) {
            System.err.println("DHT Server exception: " + e.toString());
            e.printStackTrace();
        } finally {
            registry = reg;
        }
    }

    public void startServer() {
        try {
            DhtComm stub = (DhtComm) UnicastRemoteObject.exportObject(this, 0);

            // bind the remote object's stub in the registry
            try {
                registry.bind("DhtComm", stub);
            } catch (AlreadyBoundException e) {
                registry.rebind("DhtComm", stub);
            }

            if (debug) System.out.println("DHT Server ready on " + port);
        } catch (RemoteException e) {
            System.err.println("DHT Server exception: " + e.toString());
        }
    }

    public void stopServer() {
        try {
            registry.unbind("DhtComm");
            UnicastRemoteObject.unexportObject(this, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void acceptConnection(DhtPeerAddress source) {
        try {
            if (PeerManager.allowLocalConnect && PeerManager.hasPeer(source)) {
                // locally connected
                String host = InetAddress.getLocalHost().getHostAddress();
                DhtPeerAddress newSource = new DhtPeerAddress(source.getUserID(), host,
                        source.getPort(), localPeer.localAddress.getUserID());
                localPeer.getNeighbourState().addNeighbour(newSource);
            } else {
                source.setRelative(localPeer.localAddress.getUserID());
                String clientHost = RemoteServer.getClientHost();
                // set host of source
                source.setHost(clientHost);
                localPeer.getNeighbourState().addNeighbour(source);
            }
        } catch (ServerNotActiveException  | UnknownHostException e) {
            // this is fine
        }
    }

    @Override
    public DhtPeerAddress nextHop(DhtPeerAddress source, BigInteger target) throws IOException {
        if (debug) System.out.println("server lookup");
        acceptConnection(source);
        return localPeer.getNextLocalHop(target);
    }

    @Override
    public NeighbourState getNeighbourState(DhtPeerAddress source) {
        if (debug) System.out.println("server getneighbourstate");
        acceptConnection(source);
        if (localPeer.isStable())
            return localPeer.getNeighbourState();
        else return null;
    }

    @Override
    public Long upload(DhtPeerAddress source, Integer port, BigInteger file)
            throws IOException {
        acceptConnection(source);
        DhtFile transferFile = localPeer.getDhtStore().getFile(file);
        String fileName = localPeer.getDhtStore().getFolder() + "/" + file.toString();

        Socket socket = new Socket();
        socket.setSoTimeout(1000);
        socket.connect(new InetSocketAddress(source.getHost(), port), 900);
        Thread uploader = new DirectTransfer(localPeer, source, socket, fileName, false,
                transferFile, new InternalUploadContinuation());
        uploader.start();
        return transferFile.size;
    }

    @Override
    public Integer download(DhtPeerAddress source, Integer port, DhtFile file) throws IOException {
        // return value: 0 for ACCEPT, 1 for DECLINE and 2 for REDUNDANT
        acceptConnection(source);
        file.owner.setRelative(localPeer.localAddress.getUserID());
        // only accept if owner is within predecessor range
        // or if I am between the current owner and the file, in which case I'll be the next owner
        if (!file.owner.equals(localPeer.localAddress) &&
                !localPeer.localAddress.isBetween(file.owner,
                        new DhtPeerAddress(file.hash, null, null,
                                localPeer.localAddress.getUserID())) &&
                !localPeer.getNeighbourState().getSuccessors().contains(file.owner)) {
            System.out.println("Refusing download from " + source.getConnectAddress()
                    + " with owner " + file.owner.getConnectAddress());
            return 1;
        } else if (localPeer.getDhtStore().hasFile(file))
            return 2;

        System.out.println("Storing file at " + localPeer.userName + "/" +
                file.hash.toString());

        String fileName = localPeer.getDhtStore().getFolder() + "/" + file.hash;
        Socket socket = new Socket();
        socket.setSoTimeout(1000);
        socket.connect(new InetSocketAddress(source.getHost(), port), 900);

        Thread downloader = new DirectTransfer(localPeer, source, socket, fileName, true, file,
                new InternalDownloadContinuation());
        downloader.start();
        return 0;
    }

    @Override
    public Map<BigInteger, Boolean> storingFiles(DhtPeerAddress source, List<DhtFile> files) {
        // get a list of files with their owners. Return a list of file IDs associated with
        // bools describing whether they are stored locally. Update owners in the meantime
        // if necessary
        acceptConnection(source);

        Map<BigInteger, Boolean> result = new HashMap<>();
        for (DhtFile file : files) {
            file.owner.setRelative(localPeer.localAddress.getUserID());
            boolean hasFile = localPeer.getDhtStore().hasFile(file);
            result.put(file.hash, hasFile);
            localPeer.getDhtStore().refreshResponsibility(file.hash, source, false);
        }
        return result;
    }

    @Override
    public Boolean isAlive(DhtPeerAddress source) {
        acceptConnection(source);
        return true;
    }

    @Override
    public Boolean checkUserID(DhtPeerAddress source, BigInteger userID) {
        acceptConnection(source);
        return userID.equals(localPeer.localAddress.getUserID());
    }

    @Override
    public String query(String input) {
        return localPeer.executeQuery(input);
    }
}
