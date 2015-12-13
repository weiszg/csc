package uk.ac.cam.gw361.csc;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by gellert on 08/11/2015.
 */
public class DhtNeighbourTest {

    @Test
    public void testNeighbours() {
        NeighbourState.k = 5;
        int peerCount = 50;
        int startPort = 11000;
        int k = NeighbourState.k;

        List<LocalPeer> peers = new ArrayList<>();
        TreeSet<DhtPeerAddress> addresses = new TreeSet<>();
        Map<DhtPeerAddress, LocalPeer> peerLookup = new HashMap<>();

        for (int i = 0; i < peerCount; i++) {
            LocalPeer newPeer = new LocalPeer(Integer.toString(i) + ":" +
                    Integer.toString(startPort+i), 100000);
            if (i>0) newPeer.join("localhost:" + startPort);
            peers.add(newPeer);
            addresses.add(new DhtPeerAddress(newPeer.localAddress.getUserID(),
                                             newPeer.localAddress.getHost(),
                                             newPeer.localAddress.getPort(),
                                             BigInteger.ZERO));
            peerLookup.put(newPeer.localAddress, newPeer);
        }
        List<DhtPeerAddress> sortedAddresses = new ArrayList<>(addresses);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

        for (int i = 0; i < peerCount; i++) {
            DhtPeerAddress checkedAddress = sortedAddresses.get(i);
            LocalPeer checkedPeer = peerLookup.get(checkedAddress);
            ArrayList<DhtPeerAddress> actualPredecessors =
                    checkedPeer.getNeighbourState().getPredecessors();
            ArrayList<DhtPeerAddress> actualSuccessors =
                    checkedPeer.getNeighbourState().getSuccessors();

            for (int j=-k; j<=k; j++) {
                if (j!=0 && Math.abs(j) < sortedAddresses.size()) {
                    int index = (i+j) % peerCount;
                    if (index < 0) index += peerCount;
                    int actualIndex = Math.abs(j)-1;
                    DhtPeerAddress checkWith;
                    if (j>0)
                        checkWith = actualSuccessors.get(actualIndex);
                    else
                        checkWith = actualPredecessors.get(actualIndex);
                    if (!sortedAddresses.get(index).equals(checkWith)) {
                        System.err.println("Expected: " + sortedAddresses.get(index).toString());
                        System.err.println("     got: " + checkWith.toString());
                    }
                    Assert.assertTrue(sortedAddresses.get(index).equals(checkWith));
                }
            }
        }
    }
}