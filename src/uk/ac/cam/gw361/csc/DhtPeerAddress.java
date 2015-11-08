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
    public transient BigInteger relativeSort = BigInteger.ZERO;

    public DhtPeerAddress(BigInteger userID, String host, Integer port) {
        this.userID = userID;
        this.port = port;
        this.host = host;
    }

    public void setRelative (BigInteger relative) {
        this.relativeSort = relative;
    }

    public BigInteger getUserID() { return userID; }
    public Integer getPort() { return port; }
    public String getHost() { return host; }

    @Override
    public int compareTo(DhtPeerAddress other){
        int relMe = userID.compareTo(relativeSort);
        int relOther = other.userID.compareTo(relativeSort);
        if (relMe >= 0 && relOther < 0) return -1;
        else if (relMe < 0 && relOther >= 0) return 1;
        else return userID.compareTo(other.userID);

        //return userID.subtract(relativeSort).compareTo(
          //      other.userID.subtract(relativeSort));
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

    public void print(String beginning) {
        if (userID != null)
            System.out.println(beginning + "host=" + host + " port=" + port +
                " ID=" + userID.toString());
        else
            System.out.println(beginning + "host=" + host + " port=" + port);
    }
}
