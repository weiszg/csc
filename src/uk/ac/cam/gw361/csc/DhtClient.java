package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtClient {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private Map<DhtPeerAddress, DhtComm> connections = new HashMap<>();
    //todo: TTL for lookups so that execution time is bounded

    public DhtClient(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void bootstrap(String host)
            throws IOException {
        int port = 8000;
        if (host.contains(":")) {
            port = Integer.parseInt(host.split(":")[1]);
            host = host.split(":")[0];
        }
        DhtPeerAddress pred = doLookup(new DhtPeerAddress(null, host, port), localPeer.userID);
        localPeer.getNeighbourState().addNeighbour(pred);
        localPeer.stabilise();
    }

    private DhtComm connect(DhtPeerAddress server)
            throws ConnectionFailedException {
        DhtComm comm;
        try {
            comm = doConnect(server);
        } catch (ConnectionFailedException e1) {
            System.err.println("Connection failed 1, retrying");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {}
            try {
                comm = doConnect(server);
            } catch (ConnectionFailedException e2) {
                System.err.println("Connection failed 2, running stabilise");
                localPeer.stabilise();
                try {
                    comm = doConnect(server);
                } catch (ConnectionFailedException e3) {
                    System.err.println("Connection failed 3, retrying");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {}
                    try {
                        comm = doConnect(server);
                    } catch (ConnectionFailedException e4) {
                        System.err.println("Giving up, try later");
                        throw e4;
                    }
                }
            }
        }
        return comm;
    }

    private DhtComm doConnect(DhtPeerAddress server)
            throws ConnectionFailedException {
        if (debug) server.print("client connect: ");

        // cache lookup
        if (server.getUserID() != null && connections.containsKey(server)) {
            DhtComm ret = connections.get(server);
            try {
                ret.isAlive(localPeer.localAddress);
                return ret;
            } catch (RemoteException e) {
                connections.remove(server);
            }
        }

        try {
            Registry registry = LocateRegistry.getRegistry(server.getHost(), server.getPort());
            DhtComm ret = (DhtComm) registry.lookup("DhtComm");

            //cache
            if (server.getUserID() != null)
                connections.put(server, ret);

            return ret;
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
            throw new ConnectionFailedException();
        }
    }

    public DhtPeerAddress lookup(BigInteger target)
            throws IOException {
        if (debug) System.out.println("client lookup");
        DhtPeerAddress start = localPeer.getNextHop(target);
        if (start.getHost().equals("mine"))
            return start;
        else
            return doLookup(start, target);
    }

    private DhtPeerAddress doLookup(DhtPeerAddress start, BigInteger target)
            throws IOException {
        if (debug) System.out.println("client dolookup");
        DhtPeerAddress result;
        DhtComm comm = connect(start);

        result = comm.lookup(localPeer.localAddress, target);
        if (result.getHost().equals("mine"))
            result = new DhtPeerAddress(result.getUserID(), start.getHost(), start.getPort());
        return result;
    }

    public NeighbourState getNeighbourState(DhtPeerAddress peer)
            throws IOException {
        if (debug) System.out.println("client getneighbourstate");
        NeighbourState result;
        DhtComm comm = connect(peer);
        try {
            result = comm.getNeighbourState(localPeer.localAddress);
            return result;
        } catch (RemoteException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public FileTransfer download(DhtPeerAddress peer, BigInteger file)
            throws IOException {
        if (debug) System.out.println("client download");
        DhtComm comm = connect(peer);
        FileTransfer ft;
        try {
            ServerSocket listener = new ServerSocket(0);
            FileOutputStream fos = localPeer.getFileStore().writeFile(file);
            ft = new FileTransfer(listener, fos);
            ft.start();
            Long size = comm.upload(localPeer.localAddress, listener.getLocalPort(), file);
            if (size == null)
                throw new IOException("Size null");
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }
        return ft;
    }

    public FileTransfer upload(DhtPeerAddress peer, BigInteger file)
            throws IOException {
        if (debug) System.out.println("client upload");
        DhtComm comm = connect(peer);
        FileTransfer ft;
        try {
            ServerSocket listener = new ServerSocket(0);
            FileInputStream fis = localPeer.getFileStore().readFile(file);
            ft = new FileTransfer(listener, fis);
            ft.start();
            comm.download(localPeer.localAddress, listener.getLocalPort(), file);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }
        return ft;
    }
}

class ConnectionFailedException extends IOException {

}
