package uk.ac.cam.gw361.csc.dht;

import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.TreeSet;

/**
 * Created by gellert on 10/02/2016.
 */
public class FingerState {
    private boolean debug = false;
    private static final int updateLevel = 3;
    private LocalPeer localPeer;
    private BigInteger me;
    private int[] age = new int[DhtComm.logKeySize];
    public DhtPeerAddress[] finger = new DhtPeerAddress[DhtComm.logKeySize];
    public final DhtPeerAddress[] fingerAddress = new DhtPeerAddress[DhtComm.logKeySize];

    FingerState(LocalPeer localPeer) {
        this.localPeer = localPeer;
        me = localPeer.localAddress.getUserID();
        for (int i=0; i<fingerAddress.length; i++) {
            BigInteger address = me.add(new BigInteger("2").pow(i));
            fingerAddress[i] = new DhtPeerAddress(address, null, null, me);
            age[i] = 1;
        }
    }

    private int countNulls() {
        int nulls = 0;
        for (int j=0; j<DhtComm.logKeySize; j++)
            if (finger[j]==null)
                nulls++;
        return nulls;
    }

    private int countAges() {
        int ages = 0;
        for (int j=0; j<DhtComm.logKeySize; j++)
            if (age[j]>0)
                ages++;
        return ages;
    }

    private void doAge() {
        for (int i=0; i<age.length; i++)
            age[i]++;
    }

    public synchronized void updateAll() {
        doAge();

        for (int i=0; i<DhtComm.logKeySize; i++) {
            updateOne();
            if (debug)
                System.out.println("done=" + i + ", nulls=" + countNulls() +
                        ", ages=" + countAges());
        }
        if (countAges() > 0)
            System.err.println("Fingers still out of date after updateAll");
    }

    synchronized void update() { // called from synchronized context
        for (int i=0; i<updateLevel; i++)
            updateOne();

        doAge();
    }

    private void updateOne() {
        //if (localPeer.userName.equals("9"))
        //    debug = true;

        // determine lowest useful finger
        DhtPeerAddress successor = localPeer.getNeighbourState().getImmediateSuccessor();
        if (successor == null)
            // neighbours not ready yet
            return;

        int lowestUsefulIndex = DhtComm.logKeySize;
        for (int i=0; i<fingerAddress.length; i++) {
            if (successor.compareTo(fingerAddress[i]) >= 0) {
                // finger i is useless
                finger[i] = null;
                age[i] = 0;
            } else {
                lowestUsefulIndex = i;
                break;
            }
        }

        int maxAge = -1;
        int oldestIndex = -1;

        for (int i=lowestUsefulIndex; i<age.length; i++) {
            if (age[i] > maxAge) {
                maxAge = age[i];
                oldestIndex = i;
            }
        }
        if (maxAge <= 0)
            // every finger is up to date
            return;

        // refresh oldestIndex
        try {
            DhtPeerAddress newFinger = localPeer.getClient().lookup(
                    fingerAddress[oldestIndex].getUserID(), debug, null);
            newFinger.setRelative(me);

            if (localPeer.localAddress.equals(newFinger))
                // this lookup happened when the localPeer only knew about itself
                // hence the lookup is useless
                return;

            synchronized (this) {
                if (newFinger != null) {
                    // update oldest finger to lookup value
                    if (debug) System.out.println("index " + oldestIndex + " update: " +
                            newFinger.getUserID().toString());
                    finger[oldestIndex] = newFinger;
                    age[oldestIndex] = 0;
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

    public synchronized void print(PrintStream out, String beginning) {
        out.println(beginning + "Fingers:");
        for (int i=0; i<finger.length; i++) {
            if (finger[i] != null) {
                if (i==0 || finger[i-1]==null || !finger[i-1].equals(finger[i]))
                    finger[i].print(out, beginning + "2^" + i + " -> ");
            }
        }
    }
}
