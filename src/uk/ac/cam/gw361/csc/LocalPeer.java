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
    final DhtPeerAddress localAddress;
    private DhtServer dhtServer;
    private DhtClient dhtClient;
    public DhtClient getClient() { return dhtClient; }
    private FileStore fileStore;
    public FileStore getfileStore() { return fileStore; }

    private NeighbourState neighbourState;
    public synchronized NeighbourState getNeighbourState() { return neighbourState; }
    private TreeSet<DhtPeerAddress> peers = new TreeSet<>();

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
        localAddress = new DhtPeerAddress(userID, "mine", port);
        neighbourState = new NeighbourState(localAddress);
        peers.add(localAddress);

        fileStore = new FileStore(this);
        dhtClient = new DhtClient(this);
        dhtServer = new DhtServer(this);
        dhtServer.startServer(port);
        localAddress.print("Started: ");
    }

    public synchronized void join(String remotePeerIP) {
        dhtClient.bootstrap(remotePeerIP);
    }

    public DhtPeerAddress getNextHop(BigInteger target) {
        DhtPeerAddress next = peers.lower(new DhtPeerAddress(
                target.add(BigInteger.ONE), null, null));
        if (next == null) {
            next = peers.last();
        }
        return next;
    }

    public void stabilise() {
        NeighbourState newState = new NeighbourState(localAddress);
        // todo: time limits for remote calls and failure recognition
        Set<DhtPeerAddress> asked = new HashSet<>();
        for (DhtPeerAddress neighbour : neighbourState.getNeighbours()) {
            asked.add(neighbour);
            NeighbourState remoteState = dhtClient.getNeighbourState(neighbour);
            if (remoteState != null) {
                newState.mergeNeighbourState(remoteState);
                newState.addNeighbour(neighbour);
            }
        }

        // ask new nodes too
        boolean converged = false;
        while (!converged) {
            converged = true;
            for (DhtPeerAddress neighbour : newState.getNeighbours()) {
                if (!asked.contains(neighbour)) {
                    converged = false;
                    asked.add(neighbour);
                    NeighbourState remoteState = dhtClient.getNeighbourState(neighbour);
                    if (remoteState != null) {
                        newState.mergeNeighbourState(remoteState);
                        newState.addNeighbour(neighbour);
                    }
                }
            }
        }

        neighbourState = newState;
    }
}
