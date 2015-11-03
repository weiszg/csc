package uk.ac.cam.gw361.csc;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtPeerAddress implements Comparable<DhtPeerAddress>, Serializable {
    private BigInteger userID;
    private Integer port;
    private String host;

    public DhtPeerAddress(BigInteger userID, String host, Integer port) {
        this.userID = userID;
        this.port = port;
        this.host = host;
    }

    public BigInteger getUserID() { return userID; }
    public Integer getPort() { return port; }
    public String getHost() { return host; }

    @Override
    public int compareTo(DhtPeerAddress other){
        return userID.compareTo(other.userID);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DhtPeerAddress))
            return false;
        else
            return userID.equals(((DhtPeerAddress)other).userID);
    }

    @Override
    public int hashCode() {
        return userID.hashCode();
    }

    public boolean isBetween (DhtPeerAddress a, DhtPeerAddress b) {
        //returns true if this address is on the arc from a to b on the ring
        if (a.compareTo(b) <= 0)
            return (this.compareTo(a) > 0 && this.compareTo(b) < 0);
        else
            return (this.compareTo(a) > 0 || this.compareTo(b) < 0);
    }
}
