package uk.ac.cam.gw361.csc;

import java.io.PrintStream;
import java.io.Serializable;

/**
 * Created by gellert on 11/02/2016.
 */
public class DoubleAddress implements Serializable {
    DhtPeerAddress neighbour, finger;
    transient boolean fingerAlive = false;

    DoubleAddress(DhtPeerAddress neighbour, DhtPeerAddress finger) {
        this.neighbour = neighbour;
        this.finger = finger;
    }

    public void print(PrintStream out, String beginning) {
        out.println(beginning + "n=" + neighbour.getUserID().toString() + ", f=" +
                ((finger==null) ? "null" : finger.getUserID().toString()));
    }
}
