package uk.ac.cam.gw361.csc;

import java.io.IOException;
import java.math.BigInteger;
import java.util.TreeSet;

/**
 * Created by gellert on 10/02/2016.
 */
public class FingerState {
    private static final boolean debug = true;
    private LocalPeer localPeer;
    private BigInteger me;
    private int[] age = new int[DhtComm.logKeySize];
    private DhtPeerAddress[] finger = new DhtPeerAddress[DhtComm.logKeySize];
    private DhtPeerAddress[] fingerAddress = new DhtPeerAddress[DhtComm.logKeySize];

    FingerState(LocalPeer localPeer) {
        this.localPeer = localPeer;
        me = localPeer.localAddress.getUserID();
        for (int i=0; i<fingerAddress.length; i++) {
            BigInteger address = me.subtract(new BigInteger("2").pow(i));
            fingerAddress[i] = new DhtPeerAddress(address, null, null, me);
        }
    }

    void updateOne() {
        int maxAge = -1;
        int oldestIndex = -1;

        for (int i=0; i<age.length; i++) {
            if (age[i] > maxAge) {
                maxAge = age[i];
                oldestIndex = i;
            }
        }

        // refresh oldestIndex
        try {
            DhtPeerAddress newFinger = localPeer.getClient().lookup(
                    fingerAddress[oldestIndex].getUserID());

            if (newFinger != null) {
                newFinger.setRelative(me);
                if (finger[oldestIndex] != null && !finger[oldestIndex].equals(newFinger)) {
                    // we change the old finger to something different
                    // delete the old finger from any subsequent entry
                    for (int i = oldestIndex + 1; i < finger.length; i++) {
                        if (finger[oldestIndex].equals(finger[i]))
                            finger[i] = null;
                    }
                }

                // update oldest finger to lookup value
                finger[oldestIndex] = newFinger;
                age[oldestIndex] = 0;

                // see if this is a valid finger for any subsequent entry;
                for (int i=oldestIndex+1; i< finger.length; i++) {

                    if (newFinger.compareTo(fingerAddress[i]) <= 0) {
                        finger[i] = newFinger;
                        if (debug) System.out.println("new finger also " + i + "th finger");
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Finger update failed for index " + oldestIndex + ", me=" +
                me.toString() + ", toCheck=" + fingerAddress[oldestIndex].toString() +
                    ", error: " + e.toString());
        }
    }

    synchronized TreeSet<DhtPeerAddress> getFingers() {
        TreeSet<DhtPeerAddress> shortcuts = new TreeSet<>();
        for (DhtPeerAddress f : finger) {
            if (f != null)
                shortcuts.add(f);
        }
        return shortcuts;
    }
}
