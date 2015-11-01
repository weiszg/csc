package uk.ac.cam.gw361.csc;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by gellert on 24/10/2015.
 */
public class LocalPeer {
    final String userName;
    final BigInteger userID;
    private DhtServer dhtServer;

    private TreeSet<BigInteger> neighbours = new TreeSet<>();
    private Map<BigInteger, DhtPeerAddress> hosts = new HashMap<>();
    public synchronized Map<BigInteger, DhtPeerAddress> getHosts() { return hosts; }
    public synchronized TreeSet<BigInteger> getNeighbours() { return neighbours; }

    public LocalPeer(String userName) {
        int port = 8000;
        if (userName.contains(":")) {
            port = Integer.parseInt(userName.split(":")[1]);
            userName = userName.split(":")[0];
        }

        this.userName = userName;
        MessageDigest cript = null;
        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(userName.getBytes("utf8"));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        userID = new BigInteger(cript.digest());
        neighbours.add(userID);
        hosts.put(userID, new DhtPeerAddress(userID, "mine"));

        dhtServer = new DhtServer(this);
        dhtServer.startServer(port);
    }

    public synchronized void join(String remotePeerIP) {
        DhtClient.connect(remotePeerIP);
    }
}
