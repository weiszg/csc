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
    private DhtComm comm;

    public DhtClient(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void bootstrap(String host) {
        doLookup(host, localPeer.userID);
    }

    private DhtComm connect(String host) {
        int port = 8000;
        if (host.contains(":")) {
            port = Integer.parseInt(host.split(":")[1]);
            host = host.split(":")[0];
        }
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            return (DhtComm) registry.lookup("DhtComm");
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
            return null;
        }
    }

    public DhtPeerAddress lookup(BigInteger target) {
        DhtPeerAddress start = localPeer.getNextHop(target);
        if (start == null)
            return new DhtPeerAddress(localPeer.userID, "mine");
        else
            return doLookup(start, target);
    }

    private DhtPeerAddress doLookup(DhtPeerAddress start, BigInteger target) {
        DhtPeerAddress nextHop = start;
        DhtPeerAddress currentResult = new DhtPeerAddress(null, null);
        while (!nextHop.getHost().equals(currentResult.getHost())) {
            currentResult = nextHop;
            DhtComm comm = connect(nextHop.getHost());
            try {
                nextHop = comm.getNextHop(target); //todo: refactor so that remote lookup responsible for everything will result in failures being closer to cuplrits
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }

    }
}
