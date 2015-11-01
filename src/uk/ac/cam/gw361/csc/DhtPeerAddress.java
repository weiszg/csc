package uk.ac.cam.gw361.csc;

import java.math.BigInteger;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtPeerAddress {
    private BigInteger userID;
    private String host;

    public DhtPeerAddress(BigInteger userID, String host) {
        this.userID = userID;
        this.host = host;
    }

    public BigInteger getUserID() { return userID; }
    public String getHost() { return host; }
}
