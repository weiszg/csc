package uk.ac.cam.gw361.csc;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtClient {
    private LocalPeer localPeer;
    private final boolean debug = false;
    //todo: cache connections
    //todo: TTL for lookups so that execution time is bounded

    public DhtClient(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void bootstrap(String host) {
        int port = 8000;
        if (host.contains(":")) {
            port = Integer.parseInt(host.split(":")[1]);
            host = host.split(":")[0];
        }
        DhtPeerAddress pred = doLookup(new DhtPeerAddress(null, host, port), localPeer.userID);
        localPeer.getNeighbourState().addNeighbour(pred);
        localPeer.stabilise();
    }

    private DhtComm connect(DhtPeerAddress server) {
        if (debug) server.print("client connect: ");
        try {
            Registry registry = LocateRegistry.getRegistry(server.getHost(), server.getPort());
            return (DhtComm) registry.lookup("DhtComm");
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
            return null;
        }
    }

    public DhtPeerAddress lookup(BigInteger target) {
        if (debug) System.out.println("client lookup");
        DhtPeerAddress start = localPeer.getNextHop(target);
        if (start.getHost().equals("mine"))
            return start;
        else
            return doLookup(start, target);
    }

    private DhtPeerAddress doLookup(DhtPeerAddress start, BigInteger target) {
        if (debug) System.out.println("client dolookup");
        DhtPeerAddress result = null;
        DhtComm comm = connect(start);
        try {
            result = comm.lookup(localPeer.localAddress, target);
            if (result.getHost().equals("mine"))
                result = new DhtPeerAddress(result.getUserID(), start.getHost(), start.getPort());
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return result;
    }

    public NeighbourState getNeighbourState(DhtPeerAddress peer) {
        if (debug) System.out.println("client getneighbourstate");
        NeighbourState result = null;
        DhtComm comm = connect(peer);
        try {
            result = comm.getNeighbourState(localPeer.localAddress);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return result;
    }

}
