package uk.ac.cam.gw361.csc;

import java.net.Socket;

/**
 * Created by gellert on 24/10/2015.
 */
public class Connection {
    static final int commPort = 8000;
    static final int dataPort = 8001;
    static final LocalPeer localPeer;
    private Socket socket;

    public Connection(LocalPeer localPeer, string ip) {
        Connection.localPeer = localPeer;
    }

}
