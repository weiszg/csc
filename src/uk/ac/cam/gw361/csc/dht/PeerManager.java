package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.analysis.NetworkUsageReporter;
import uk.ac.cam.gw361.csc.dht.DhtComm;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalDhtCommWrapper;
import uk.ac.cam.gw361.csc.dht.LocalPeer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gellert on 19/01/2016.
 */
public class PeerManager {
    // localConnect defines whether peers are connected by normal function calls and without RMI
    // if both the caller and the callee live
    static public boolean allowLocalConnect = true;

    private static Map<DhtPeerAddress, LocalPeer> addressLookup = new HashMap<>();
    private static Map<Integer, LocalPeer> portLookup = new HashMap<>();
    private static Connector connector = new Connector();

    private static Long rmiBytesReceived = new Long(0);
    private static Long tcpBytesReceived = new Long(0);
    private static Long rmiBytesSent = new Long(0);
    private static Long tcpBytesSent = new Long(0);

    private static NetworkLogger logger;
    public static boolean perfmon = false;

    public static LocalPeer spawnPeer(String userName, long stabiliseInterval) {
        LocalPeer localPeer = new LocalPeer(userName, stabiliseInterval);
        addressLookup.put(localPeer.localAddress, localPeer);
        portLookup.put(localPeer.localAddress.getPort(), localPeer);
        return localPeer;
    }

    public static synchronized void setConnector(Connector newConnector) {
        connector = newConnector;
    }

    public static synchronized Connector getConnector() {
        return connector;
    }

    public static synchronized void startLogging() {
        logger = new NetworkLogger();
        logger.start();
    }

    public static synchronized void reportBytesSent(boolean in, long length, boolean tcp) {
        if (in)
            if (tcp)
                tcpBytesReceived += length;
            else
                rmiBytesReceived += length;
        else
            if (tcp)
                tcpBytesSent += length;
            else
                rmiBytesSent += length;
    }

    static synchronized String[] getTotalTraffic() {
        return new String[]{((Long)System.currentTimeMillis()).toString(),
                rmiBytesReceived.toString(), rmiBytesSent.toString(),
                tcpBytesReceived.toString(), tcpBytesSent.toString()};
    }

    static void removePeer(DhtPeerAddress address) {
        addressLookup.remove(address);
        portLookup.remove(address.getPort());
    }

    static boolean hasPeer(DhtPeerAddress address) {
        return addressLookup.containsKey(address);
    }

    static boolean hasPeer(Integer localPort) {
        return portLookup.containsKey(localPort);
    }

    static public LocalPeer getPeer(DhtPeerAddress address) {
        return addressLookup.get(address);
    }

    static DhtComm getServer(DhtPeerAddress address) {
        return new LocalDhtCommWrapper(addressLookup.get(address).getServer());
    }

    static DhtComm getServer(Integer localPort) {
        return new LocalDhtCommWrapper(portLookup.get(localPort).getServer());
    }
}

class NetworkLogger extends Thread {
    static NetworkUsageReporter reporter = new NetworkUsageReporter("net.csv");

    public void run() {
        while (true) {
            reporter.report(PeerManager.getTotalTraffic());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
    }
}
