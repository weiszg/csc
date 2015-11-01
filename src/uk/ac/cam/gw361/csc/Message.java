package uk.ac.cam.gw361.csc;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * Created by gellert on 24/10/2015.
 */
public class Message implements Serializable {
    static final long serialVersionUID = 1L;
    final string senderUserName;
    final BigInteger senderUserID;

    public Message(LocalPeer localPeer) {
        senderUserName = localPeer.userName;
        senderUserID = localPeer.userID;
    }
}
