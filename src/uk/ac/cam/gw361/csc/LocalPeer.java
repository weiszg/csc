package uk.ac.cam.gw361.csc;

import java.io.IOException;
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
    public FileStore getFileStore() { return fileStore; }

    private NeighbourState neighbourState;
    public synchronized NeighbourState getNeighbourState() { return neighbourState; }
    private TreeSet<DhtPeerAddress> peers = new TreeSet<>();
    private Boolean stabilising = false;

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
        try {
            dhtClient.bootstrap(remotePeerIP);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Failed to connect to DHT pool");
        }
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
        synchronized (this) {
            if (!stabilising) {
                doStabilise();
                return;
            }
        }
        while (true) {
            synchronized (this) {
                if (!stabilising)
                    return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {}
        }
    }

    private void doStabilise() {
        synchronized (this) {
            stabilising = true;
        }

        NeighbourState newState = new NeighbourState(localAddress);
        // todo: time limits for remote calls and failure recognition
        Set<DhtPeerAddress> asked = new HashSet<>();
        for (DhtPeerAddress neighbour : neighbourState.getNeighbours()) {
            asked.add(neighbour);
            try {
                NeighbourState remoteState = dhtClient.getNeighbourState(neighbour);
                newState.mergeNeighbourState(remoteState);
                newState.addNeighbour(neighbour);
            } catch (IOException e) {
                // this is fine, we won't add this failing peer
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
                    try {
                        NeighbourState remoteState = dhtClient.getNeighbourState(neighbour);
                        newState.mergeNeighbourState(remoteState);
                        newState.addNeighbour(neighbour);
                    } catch (IOException e) {
                        // this is fine, we won't add this failing peer
                    }
                }
            }
        }

        neighbourState = newState;
        synchronized (this) {
            stabilising = false;
        }
    }

    public FileTransfer getFile(BigInteger file) throws IOException {
        FileTransfer ft = null;
        DhtPeerAddress target = dhtClient.lookup(file);
        if (file != null)
            ft = dhtClient.download(target, file);
        return ft;
    }

    public FileTransfer publishFile(BigInteger file) throws IOException {
        FileTransfer ft = null;
        DhtPeerAddress target = dhtClient.lookup(file);
        if (file != null)
            ft = dhtClient.upload(target, file);
        return ft;
    }
}
