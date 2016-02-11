package uk.ac.cam.gw361.csc;

import java.io.Serializable;

/**
 * Created by gellert on 11/02/2016.
 */
public class DoubleAddress implements Serializable {
    DhtPeerAddress neighbour, finger;
    transient boolean fingerAlive = true;

    DoubleAddress(DhtPeerAddress neighbour, DhtPeerAddress finger) {
        this.neighbour = neighbour;
        this.finger = finger;
    }
}
