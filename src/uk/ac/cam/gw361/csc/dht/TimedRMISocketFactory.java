package uk.ac.cam.gw361.csc.dht;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.server.RMISocketFactory;

/**
 * Created by gellert on 21/02/2016.
 */
public class TimedRMISocketFactory extends RMISocketFactory {
    int timeout = 1000;
    public Socket createSocket(String host, int port) throws IOException
    {
        Socket socket = new Socket();
        socket.setSoTimeout(timeout);
        socket.setSoLinger(true, timeout);
        // use PeerManager's connector to allow proxies
        PeerManager.getConnector().connect(socket, new InetSocketAddress(host, port), timeout);
        return socket;
    }

    public ServerSocket createServerSocket(int port)
            throws IOException
    {
        return new ServerSocket(port);
    }
}
