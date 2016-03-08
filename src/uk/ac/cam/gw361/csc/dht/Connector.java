package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.proxy.Proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by gellert on 22/02/2016.
 */
public class Connector {
    // used to connect to remote peers

    public void connect(Socket s, InetSocketAddress address, int timeout) throws  IOException {
        s.connect(address, timeout);
    }

    public void connect(Socket s, String host, int port) throws IOException {
        s.connect(new InetSocketAddress(host, port));
    }

    Registry getRegistry(String host, int port) throws RemoteException {
        return LocateRegistry.getRegistry(host, port);
    }
}
