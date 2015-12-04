package uk.ac.cam.gw361.csc;

import com.sun.org.apache.xpath.internal.operations.Bool;

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
    private Boolean stabilising = false;

    private Set<FileTransfer> runningTransfers = new HashSet<>();

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
        localAddress = new DhtPeerAddress(userID, "localhost", port);
        neighbourState = new NeighbourState(localAddress);

        fileStore = new FileStore(this);
        dhtClient = new DhtClient(this);
        dhtServer = new DhtServer(this, port);
        dhtServer.startServer();
        localAddress.print("Started: ");
    }

    public synchronized void join(String remotePeerIP) {
        try {
            dhtClient.bootstrap(remotePeerIP);
            System.out.println("Connected to DHT pool");
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Failed to connect to DHT pool");
        }
    }

    public DhtPeerAddress getNextLocalHop(BigInteger target) {
        TreeSet<DhtPeerAddress> peers = neighbourState.getNeighbours();
        peers.add(localAddress);

        DhtPeerAddress next = peers.lower(new DhtPeerAddress(target, null, null));
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
        // todo: time limits for remote calls and failure recognition
        synchronized (this) {
            stabilising = true;
        }

        NeighbourState newState = new NeighbourState(localAddress);
        Set<DhtPeerAddress> candidates = neighbourState.getNeighbours();
        Set<DhtPeerAddress> asked = new HashSet<>();

        while (!candidates.isEmpty()) {
            Set<DhtPeerAddress> newCandidates = new HashSet<>();

            for (Iterator<DhtPeerAddress> iterator = candidates.iterator(); iterator.hasNext(); ) {
                DhtPeerAddress candidate = iterator.next();
                iterator.remove();
                if (!asked.contains(candidate) && neighbourState.isClose(candidate)) {
                    asked.add(candidate);
                    try {
                        NeighbourState remoteState = dhtClient.getNeighbourState(candidate);
                        newCandidates.addAll(remoteState.getNeighbours());
                        newState.addNeighbour(candidate);
                    } catch (IOException e) {
                        // this is fine, we won't add this failing peer
                        System.err.println("Failing link " + localAddress.getHost() + ":" +
                                localAddress.getPort() + " - " + candidate.getHost() + ":" +
                                candidate.getPort());
                    }
                }
            }
            candidates = newCandidates;
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
        runningTransfers.add(ft);
        return ft;
    }

    public FileTransfer publishFile(String file) throws IOException {
        BigInteger hash = FileHasher.hashFile(file);
        FileTransfer ft = null;
        DhtPeerAddress target = dhtClient.lookup(hash);
        if (file != null)
            ft = dhtClient.upload(target, hash, file);
        runningTransfers.add(ft);
        return ft;
    }

    synchronized void notifyTransferCompleted(FileTransfer ft, boolean success) {
        runningTransfers.remove(ft);
    }

    synchronized void disconnect() {
        dhtServer.stopServer();
    }
}
