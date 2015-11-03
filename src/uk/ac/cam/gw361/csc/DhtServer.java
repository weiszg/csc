package uk.ac.cam.gw361.csc;

import java.math.BigInteger;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
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
        System.out.println("server lookup");
        acceptConnection(source);
        return localPeer.getClient().lookup(target);
    }

    public NeighbourState getNeighbourState(DhtPeerAddress source) {
        System.out.println("server getneighbourstate");
        acceptConnection(source);
        return localPeer.getNeighbourState();
    }
}
