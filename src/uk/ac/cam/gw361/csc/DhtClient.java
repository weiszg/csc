package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtClient {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private Map<DhtPeerAddress, DhtComm> connections = new HashMap<>();

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
        DhtPeerAddress pred = doLookup(new DhtPeerAddress(null, host, port,
                        localPeer.localAddress.getUserID()), localPeer.localAddress.getUserID());
        localPeer.getNeighbourState().addNeighbour(pred);
        localPeer.stabilise();
    }

    private DhtComm connect(DhtPeerAddress server) throws ConnectionFailedException {
        if (server.equals(localPeer.localAddress)) return localPeer.getServer();
        DhtComm comm;
        try {
            comm = doConnect(server);
        } catch (ConnectionFailedException e1) {
            if (debug) System.err.println("Connection failed 1, retrying");
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
            }
            try {
                comm = doConnect(server);
            } catch (ConnectionFailedException e2) {
                if (debug) System.err.println("Connection failed 2, retrying");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                }
                try {
                    comm = doConnect(server);
                } catch (ConnectionFailedException e3) {
                    if (debug) System.err.println("Giving up, try later");
                    throw e3;
                }
            }
        }
        return comm;
    }

    private DhtComm doConnect(DhtPeerAddress server)
            throws ConnectionFailedException {
        if (debug) server.print(System.out, "client connect: ");

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
            if (debug) System.err.println("Client exception: " + e.toString());
            throw new ConnectionFailedException();
        }
    }

    public DhtPeerAddress lookup(BigInteger target)
            throws IOException {
        // navigate to the highest peer lower than the target
        if (debug) System.out.println("client lookup");
        DhtPeerAddress start = localPeer.getNextLocalHop(target);
        if (start.getHost().equals("localhost"))
            return start;
        else
            return doLookup(start, target);
    }

    private DhtPeerAddress doLookup(DhtPeerAddress start, BigInteger target)
            throws IOException {
        if (debug) System.out.println("client dolookup");
        DhtPeerAddress nextHop = start, prevHop = null;

        do {
            prevHop = nextHop;
            DhtComm comm = connect(prevHop);
            nextHop = comm.nextHop(localPeer.localAddress, target);
            if (nextHop.getHost().equals("localhost"))
                nextHop = new DhtPeerAddress(nextHop.getUserID(),
                        nextHop.getHost(), nextHop.getPort(), localPeer.localAddress.getUserID());
        } while (!nextHop.equals(prevHop));
        return nextHop;
    }

    public NeighbourState getNeighbourState(DhtPeerAddress peer)
            throws IOException {
        if (debug) System.out.println("client getneighbourstate");
        NeighbourState result;
        DhtComm comm = connect(peer);
        try {
            result = comm.getNeighbourState(localPeer.localAddress);
            result.setLocalAddress(localPeer.localAddress);
            return result;
        } catch (RemoteException e) {
            if (debug) e.printStackTrace();
            throw e;
        }
    }

    public Map<BigInteger, Boolean> storingFiles(DhtPeerAddress peer, List<DhtFile> files)
            throws IOException {
        if (debug) System.out.println("client getstoringfiles");
        Map<BigInteger, Boolean> result;
        DhtComm comm = connect(peer);
        try {
            result = comm.storingFiles(localPeer.localAddress, files);
            return result;
        } catch (RemoteException e) {
            if (debug) e.printStackTrace();
            throw e;
        }
    }

    public DhtTransfer download(DhtPeerAddress peer, BigInteger file)
            throws IOException {
        if (debug) System.out.println("client download");
        DhtComm comm = connect(peer);
        DhtTransfer ft;
        try {
            ServerSocket listener = new ServerSocket(0);
            FileOutputStream fos = new FileOutputStream(file.toString());
            // the owner doesn't matter, the destination of the download could be a different folder
            ft = new DhtTransfer(localPeer, peer, listener, fos, file, null);
            ft.start();
            Long size = comm.upload(localPeer.localAddress, listener.getLocalPort(), file);
            if (size == null)
                throw new IOException("Size null");
        } catch (IOException ioe) {
            if (debug) ioe.printStackTrace();
            throw ioe;
        }
        return ft;
    }

    public DhtTransfer upload(DhtPeerAddress peer, BigInteger file,
                               DhtPeerAddress owner) throws IOException {
        FileInputStream fis = localPeer.getDhtStore().readFile(file);
        return doUpload(peer, file, fis, owner);
    }

    public DhtTransfer upload(DhtPeerAddress peer, BigInteger file, String name,
                               DhtPeerAddress owner)
            throws IOException {
        FileInputStream fis = new FileInputStream(name);
        return doUpload(peer, file, fis, owner);
    }

    public DhtTransfer doUpload(DhtPeerAddress peer, BigInteger file, FileInputStream fis,
                                 DhtPeerAddress owner)
            throws IOException {
        if (debug) System.out.println("client upload");
        DhtComm comm = connect(peer);
        DhtTransfer ft;
        try {
            ServerSocket listener = new ServerSocket(0);
            ft = new DhtTransfer(localPeer, peer, listener, fis, file, null);
            ft.start();

            if (!comm.download(localPeer.localAddress, listener.getLocalPort(), file, owner))
                System.out.println("Me " + localPeer.localAddress.getPort() +
                        " of range for receiver " + peer.getPort());
        } catch (IOException ioe) {
            if (debug) ioe.printStackTrace();
            throw ioe;
        }
        return ft;
    }

    public String query(DhtPeerAddress peer, String input) throws IOException {
        if (debug) System.out.println("client query");
        DhtComm comm = connect(peer);
        return comm.query(input);
    }
}

class ConnectionFailedException extends IOException {

}
