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

public class ProxiedConnector extends Connector {
    int latency;
    long bandwidth;

    public ProxiedConnector(int latency, long bandwidth) {
        this.latency = latency;
        this.bandwidth = bandwidth;
    }

    public void connect(Socket s, InetSocketAddress address, int timeout) throws  IOException {
        Proxy sp = new Proxy(0, address.getHostName(), address.getPort(), latency, bandwidth);
        s.connect(new InetSocketAddress("127.0.0.1", sp.getLocalPort()), timeout);
    }

    public void connect(Socket s, String host, int port) throws IOException {
        Proxy sp = new Proxy(0, host, port, latency, bandwidth);
        s.connect(new InetSocketAddress("127.0.0.1", sp.getLocalPort()));
    }

    Registry getRegistry(String host, int port) throws RemoteException {
        Proxy sp = new Proxy(0, host, port, latency, bandwidth);
        return LocateRegistry.getRegistry("127.0.0.1", sp.getLocalPort());
    }
}
