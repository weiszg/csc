package uk.ac.cam.gw361.csc.storage;

import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;

import java.math.BigInteger;

/**
 * Created by gellert on 21/02/2016.
 */
public class SignedFile extends DhtFile {
    public Long timestamp;

    public SignedFile(BigInteger hash, Long size, DhtPeerAddress owner, Long timestamp) {
        super(hash, size, owner);
        this.timestamp = timestamp;
    }

    public SignedFile(SignedFile toCopy) {
        super(toCopy);
        this.timestamp = toCopy.timestamp;
    }

    public boolean checkHash(BigInteger expectedHash) {
        // signed files always pass the hash check - they aren't hash addressed
        return true;
    }
}

