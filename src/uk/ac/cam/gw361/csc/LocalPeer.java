package uk.ac.cam.gw361.csc;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by gellert on 24/10/2015.
 */
public class LocalPeer {
    final String userName;
    final BigInteger userID;
    private DhtServer dhtServer;
    private DhtClient dhtClient;

    private TreeSet<BigInteger> peers = new TreeSet<>();
    private Map<Integer, BigInteger> neighbours = new TreeMap<>();
    private Map<BigInteger, DhtPeerAddress> hosts = new HashMap<>();
    public synchronized Map<BigInteger, DhtPeerAddress> getHosts() { return hosts; }
    private synchronized TreeSet<BigInteger> getPeers() { return peers; }

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
        peers.add(userID);
        neighbours.add(0, userID);
        hosts.put(userID, new DhtPeerAddress(userID, "mine"));

        dhtServer = new DhtServer(this);
        dhtServer.startServer(port);
    }

    //todo: initialize dhtClient on first incoming connection if not yet initialized
    public synchronized void join(String remotePeerIP) {
        dhtClient = new DhtClient(this);
        dhtClient.connect(remotePeerIP);
    }

    //todo: refactor this into DhtClient so that this method is responsible for performing the whole lookup
    public DhtPeerAddress getNextHop(BigInteger target) {
        BigInteger nextID = getPeers().lower(target.add(BigInteger.ONE));
        if (nextID == null) {
            nextID = getPeers().last();
        }
        return getHosts().getOrDefault(nextID, null);
    }
}
