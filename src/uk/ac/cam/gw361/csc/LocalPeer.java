package uk.ac.cam.gw361.csc;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by gellert on 24/10/2015.
 */
public class LocalPeer {
    final string userName;
    final BigInteger userID;

    public LocalPeer(string username, string remotePeerIP) {

        MessageDigest cript = null;
        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(username.getBytes("utf8"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        userID = new BigInteger(cript.digest());


    }
}
