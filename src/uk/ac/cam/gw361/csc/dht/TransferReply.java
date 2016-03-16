package uk.ac.cam.gw361.csc.dht;

import java.io.Serializable;

/**
 * Created by gellert on 16/03/2016.
 */
public class TransferReply implements Serializable {
    Long primary;
    Integer port;

    TransferReply(Long primary, Integer port) {
        this.primary = primary;
        this.port = port;
    }

    TransferReply(Integer primary, Integer port) {
        this.primary = new Long(primary);
        this.port = port;
    }
}
