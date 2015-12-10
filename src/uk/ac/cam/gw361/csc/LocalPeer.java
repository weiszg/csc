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
        System.out.println("stabilise");
        // todo: time limits for remote calls and failure recognition
        // todo: transfer ownership for files whose owners disappeared
        synchronized (this) {
            stabilising = true;
        }

        NeighbourState newState = new NeighbourState(localAddress);
        Set<DhtPeerAddress> candidates = neighbourState.getNeighbours();
        Set<DhtPeerAddress> asked = new HashSet<>();
        Set<DhtPeerAddress> failingPeers = new HashSet<>();

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
                        // candidate is failing, migrate responsibilities
                        failingPeers.add(candidate);
                        System.err.println("Failing link " + localAddress.getHost() + ":" +
                                localAddress.getPort() + " - " + candidate.getHost() + ":" +
                                candidate.getPort());
                    }
                }
            }
            candidates = newCandidates;
        }

        neighbourState = newState;
        migrateResponsibilities(failingPeers);
        stabiliseReplicas();

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
            ft = dhtClient.upload(target, hash, file, target);
        runningTransfers.add(ft);
        return ft;
    }

    private void migrateResponsibilities(Set<DhtPeerAddress> failingPeers) {
        // set all foster file's responsibility to me for the time being
        // stabiliseReplicas will communicate with successors to establish who the real owner is
        for (DhtPeerAddress peer : failingPeers) {
            List<DhtFile> fosterFiles = fileStore.getResponsibilitiesFor(peer);
            for (DhtFile file : fosterFiles)
                fileStore.refreshResponsibility(file.fileHash, localAddress, true);
        }
    }

    private void stabiliseReplicas() {
        List<DhtFile> myFiles = fileStore.getResponsibilitiesFor(localAddress);
        Set<DhtPeerAddress> neighbours = neighbourState.getNeighbours();
        Set<BigInteger> doNotTransfer = new HashSet<>();
        HashMap<BigInteger, List<DhtPeerAddress>> transfers = new HashMap<>();

        // first talk to neighbours to determine who has what, what needs to be replicated
        for (DhtPeerAddress p : neighbours) {
            try {
                Map<BigInteger, Boolean> stored = dhtClient.storingFiles(p, myFiles);
                for (BigInteger file : stored.keySet())
                    // we are only interested in replicating if peer is a predecessor
                    // or if it is between us and the file
                    if (neighbourState.isPredecessor(p) ||
                            p.isBetween(localAddress, new DhtPeerAddress(file, null, null))) {

                        if (!stored.get(file)) {
                            // add to transfers
                            if (!transfers.containsKey(file)) {
                                LinkedList<DhtPeerAddress> ll = new LinkedList<>();
                                ll.add(p);
                                transfers.put(file, ll);
                            } else
                                transfers.get(file).add(p);
                        } else
                            if (fileStore.refreshResponsibility(file, p, false))
                                // this means one of our successors has the file therefore
                                // we are wrong in believing that we are the owners
                                // hence prevent all transfers of this file
                                doNotTransfer.add(file);
                    }
            } catch (IOException e) {
                // if a neighbour is not responding, the next synchronisation loop will take care
                e.printStackTrace();
            }
        }

        // do all the transfers
        for (BigInteger file : transfers.keySet()) {
            if (!doNotTransfer.contains(file))
                for (DhtPeerAddress remotePeer : transfers.get(file)) {
                    try {
                        FileTransfer ft = dhtClient.upload(remotePeer, file, localAddress);
                        runningTransfers.add(ft);
                        // when transfer finishes, make it the new owner if is between me and file
                    } catch (IOException e) {
                        // no replication to failing link, the link will be deleted when
                        // we synchronize next
                        e.printStackTrace();
                    }
                }
        }
    }

    void replicate(BigInteger file) throws IOException {
        List<DhtPeerAddress> predecessors = neighbourState.getPredecessors();
        FileTransfer ft = null;
        for (DhtPeerAddress p : predecessors) {
            ft = dhtClient.upload(p, file, localAddress);
            runningTransfers.add(ft);
        }
    }

    synchronized void notifyTransferCompleted(FileTransfer ft, boolean success) {
        runningTransfers.remove(ft);
    }

    synchronized void disconnect() {
        dhtServer.stopServer();
    }
}
