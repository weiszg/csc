package uk.ac.cam.gw361.csc.storage;

import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by gellert on 21/02/2016.
 */
public class DhtFile implements Serializable {
    public BigInteger hash;
    public Long size;
    public DhtPeerAddress owner;
    transient Date lastQueried;

    public DhtFile(BigInteger hash, Long size, DhtPeerAddress owner) {
        this.hash = hash;
        this.size = size;
        this.owner = owner;
        this.lastQueried = new Date();
    }

    public DhtFile(DhtFile toCopy) {
        this.hash = toCopy.hash;
        this.size = toCopy.size;
        this.owner = toCopy.owner;
        this.lastQueried = toCopy.lastQueried;
    }

    public boolean checkHash(BigInteger expectedHash) {
        return (expectedHash != null && hash.equals(expectedHash));
    }
}