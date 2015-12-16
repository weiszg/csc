package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
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
            String clientHost = RemoteServer.getClientHost();
            localPeer.getNeighbourState().addNeighbour(
                    new DhtPeerAddress(source.getUserID(), clientHost, source.getPort(),
                            localPeer.localAddress.getUserID()));
        } catch (ServerNotActiveException e) {
            // this is fine
        }
    }

    public DhtPeerAddress nextHop(DhtPeerAddress source, BigInteger target) throws IOException {
        if (debug) System.out.println("server lookup");
        acceptConnection(source);
        return localPeer.getNextLocalHop(target);
    }

    public NeighbourState getNeighbourState(DhtPeerAddress source) {
        if (debug) System.out.println("server getneighbourstate");
        acceptConnection(source);
        return localPeer.getNeighbourState();
    }

    public Long upload(DhtPeerAddress source, Integer port, BigInteger file)
            throws IOException {
        acceptConnection(source);
        Long size = localPeer.getFileStore().getLength(file);
        FileInputStream fis = localPeer.getFileStore().readFile(file);

        Socket socket = new Socket(source.getHost(), port);
        Thread uploader = new FileTransfer(localPeer, source, socket, fis, file);
        uploader.start();
        return size;
    }

    public Boolean download(DhtPeerAddress source, Integer port, BigInteger file,
                            DhtPeerAddress owner) throws IOException {
        acceptConnection(source);
        owner.setRelative(localPeer.localAddress.getUserID());
        // only accept if owner is within predecessor range
        if (!owner.equals(localPeer.localAddress) &&
                !localPeer.getNeighbourState().getSuccessors().contains(owner))
            return false;

        System.out.println("Storing file at " + localPeer.userName + "/" +
                file.toString());
        FileOutputStream fos = localPeer.getFileStore().writeFile(file);

        Socket socket = new Socket(source.getHost(), port);
        Thread downloader = new FileTransfer(localPeer, source, socket, fos, file, owner);
        downloader.start();
        return true;
    }

    public Map<BigInteger, Boolean> storingFiles(List<DhtFile> files) {
        // get a list of files with their owners. Return a list of file IDs associated with
        // bools describing whether they are stored locally. Update owners in the meantime
        // if necessary
        Map<BigInteger, Boolean> result = new HashMap<>();
        for (DhtFile file : files) {
            file.owner.setRelative(localPeer.localAddress.getUserID());
            boolean contains = localPeer.getFileStore().containsFile(file.fileHash);
            result.put(file.fileHash, contains);
            localPeer.getFileStore().refreshResponsibility(file.fileHash, file.owner, false);
        }
        return result;
    }

    public Boolean isAlive(DhtPeerAddress source) {
        acceptConnection(source);
        return true;
    }
}
