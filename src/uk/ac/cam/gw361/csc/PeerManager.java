package uk.ac.cam.gw361.csc;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gellert on 19/01/2016.
 */
public class PeerManager {
    // localConnect defines whether peers are connected by normal function calls and without RMI
    // if both the caller and the callee live
    static boolean allowLocalConnect = true;

    private static Map<DhtPeerAddress, LocalPeer> addressLookup = new HashMap<>();
    private static Map<Integer, LocalPeer> portLookup = new HashMap<>();

    static LocalPeer spawnPeer(String userName, long stabiliseInterval) {
        LocalPeer localPeer = new LocalPeer(userName, stabiliseInterval);
        addressLookup.put(localPeer.localAddress, localPeer);
        portLookup.put(localPeer.localAddress.getPort(), localPeer);
        return localPeer;
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

    static LocalPeer getPeer(DhtPeerAddress address) {
        return addressLookup.get(address);
    }

    static DhtComm getServer(DhtPeerAddress address) {
        return new LocalDhtCommWrapper(addressLookup.get(address).getServer());
    }

    static DhtComm getServer(Integer localPort) {
        return new LocalDhtCommWrapper(portLookup.get(localPort).getServer());
    }
}
