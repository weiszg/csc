package uk.ac.cam.gw361.csc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;
import java.util.*;

/**
 * Created by gellert on 08/11/2015.
 */
public class DhtNeighbourTest {
    static int peerCount = 200;
    static int startPort = 10000;
    static int k;
    static TreeSet<DhtPeerAddress> addresses;
    static List<DhtPeerAddress> sortedAddresses;

    @BeforeClass
    public static void setUpBeforeClass() {
        NeighbourState.k = 5;
        k = NeighbourState.k;
        addresses = new TreeSet<>();

        for (int i = 0; i < peerCount; i++) {
            LocalPeer newPeer = PeerManager.spawnPeer(Integer.toString(i) + ":" +
                    Integer.toString(startPort + i), 100000);
            if (i>0) newPeer.join("localhost:" + startPort);
            addresses.add(new DhtPeerAddress(newPeer.localAddress.getUserID(),
                    newPeer.localAddress.getHost(),
                    newPeer.localAddress.getPort(),
                    BigInteger.ZERO));
        }
        sortedAddresses = new ArrayList<>(addresses);

        try { Thread.sleep(1000); } catch (InterruptedException ie) { Assert.assertTrue(false); }
    }

    @Test
    public void testNeighbours() {
        for (int i = 0; i < peerCount; i++) {
            DhtPeerAddress checkedAddress = sortedAddresses.get(i);
            LocalPeer checkedPeer = PeerManager.getPeer(checkedAddress);
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

    @Test
    public void testFingers() {
        for (int i = 0; i < peerCount; i++) {
            DhtPeerAddress checkedAddress = sortedAddresses.get(i);
            LocalPeer checkedPeer = PeerManager.getPeer(checkedAddress);
            checkedPeer.getFingerState().updateAll();
            DhtPeerAddress[] checkedFingers = checkedPeer.getFingerState().finger;

            // construct sorted address set relative to checkedAddress
            TreeSet<DhtPeerAddress> relativeAddresses = new TreeSet<>();
            for (DhtPeerAddress a : addresses) {
                relativeAddresses.add(new DhtPeerAddress(
                        a.getUserID(), a.getHost(), a.getPort(), checkedAddress.getUserID()));
            }

            for (int j=0; j<DhtComm.logKeySize; j++) {
                BigInteger fingerID = checkedAddress.getUserID().subtract(
                        new BigInteger("2").pow(j));
                DhtPeerAddress fingerAddress = new DhtPeerAddress(fingerID, null, null,
                        checkedAddress.getUserID());

                if (!fingerAddress.equals(
                        checkedPeer.getFingerState().fingerAddress[j])) {
                    System.err.println("Expected: " + fingerAddress.getUserID().toString());
                    System.err.println("     got: " +
                            checkedPeer.getFingerState().fingerAddress[j].getUserID().toString());
                }
                Assert.assertTrue(fingerAddress.equals(
                        checkedPeer.getFingerState().fingerAddress[j]));

                DhtPeerAddress actualFinger = relativeAddresses.lower(fingerAddress);
                if (!actualFinger.equals(checkedFingers[j])) {
                    System.err.println("Expected: " + actualFinger.getUserID().toString());
                    System.err.println("     got: " + ((checkedFingers[j] == null) ? "null" :
                            checkedFingers[j].getUserID().toString()));
                }

                Assert.assertTrue(actualFinger.equals(checkedFingers[j]));
            }

            checkedAddress.print(System.out, "");
            checkedPeer.getFingerState().print(System.out, "");
        }
    }
}
