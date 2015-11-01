package uk.ac.cam.gw361.csc;

import java.math.BigInteger;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;


/**
 * Created by gellert on 01/11/2015.
 */
public class DhtServer implements DhtComm {
    private LocalPeer localPeer;

    public DhtServer(LocalPeer localPeer) {
        this.localPeer = localPeer;

    }

    public String sayHello() {
        return "Hello, world!";
    }

    public DhtPeerAddress getNextHop(BigInteger target) {
        BigInteger nextID = localPeer.getNeighbours().lower(target.subtract(BigInteger.ONE));
        if (nextID == null) {
            nextID = localPeer.getNeighbours().last();
        }
        DhtPeerAddress nextHop = localPeer.getHosts().getOrDefault(nextID, null);
        return nextHop;
    }

    public Set<BigInteger> getNeighbours() {
        return localPeer.getNeighbours();
    }

    public void startServer(int localPort) {
        try {
            DhtComm stub = (DhtComm) UnicastRemoteObject.exportObject(this, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.createRegistry(localPort);
            registry.bind("DhtComm", stub);

            System.err.println("DHT Server ready");
        } catch (Exception e) {
            System.err.println("DHT Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

}
