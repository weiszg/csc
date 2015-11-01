package uk.ac.cam.gw361.csc;

import java.io.Serializable;

/**
 * Created by gellert on 24/10/2015.
 */
public class AnnounceMessage extends Message implements Serializable {
    string request;
    public AnnounceMessage(LocalPeer localPeer, string request) {
        super(localPeer);
        this.request = request;
    }

}
