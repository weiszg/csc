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

    private static Map<DhtPeerAddress, LocalPeer> peerLookup = new HashMap<>();

    static LocalPeer spawnPeer(String userName, long stabiliseInterval) {
        LocalPeer localPeer = new LocalPeer(userName, stabiliseInterval);
        peerLookup.put(localPeer.localAddress, localPeer);
        return localPeer;
    }

    static void removePeer(DhtPeerAddress address) {
        peerLookup.remove(address);
    }

    static boolean hasPeer(DhtPeerAddress address) {
        return peerLookup.containsKey(address);
    }

    static LocalPeer getPeer(DhtPeerAddress address) {
        return peerLookup.get(address);
    }

    static DhtComm getServer(DhtPeerAddress address) {
        return new LocalDhtCommWrapper(peerLookup.get(address).getServer());
    }
}
