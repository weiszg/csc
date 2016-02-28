package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.transfer.DirectTransfer;
import uk.ac.cam.gw361.csc.transfer.InternalUploadContinuation;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by gellert on 10/12/2015.
 */
public class Stabiliser extends Thread {
    private Boolean stabilising = false;
    private LocalPeer localPeer;
    private boolean debug = false;
    private long interval;
    private boolean running = true;
    private String bootstrapPeer = null;
    private long lastStabilised = System.nanoTime() / 1000000;

    boolean isStable() {
        return (System.nanoTime() / 1000000 - lastStabilised <= interval * 2);
    }

    public Stabiliser(LocalPeer localPeer, Long interval) {
        this.localPeer = localPeer;
        this.interval = interval;
        this.start();
    }

    public void disconnect() {
        synchronized (this) { running = false; }
    }
    
    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ie) {
            }
            try {
                stabilise();
            } catch (Exception e) {
                System.err.println("Stabilisation core error at " +
                        localPeer.localAddress.getConnectAddress());
                e.printStackTrace();
            }
        }
    }

    void setJoin(String remotePeerIP) {
        this.bootstrapPeer = remotePeerIP;
    }

    private void bootstrap() {
        if (bootstrapPeer==null) return;
        try {
            localPeer.getClient().bootstrap(bootstrapPeer);
            System.out.println(localPeer.localAddress.getConnectAddress()
                    + ": connected to DHT pool");
            try {
                // try fetching my file list
                localPeer.getFileList(localPeer.userName, localPeer.publicKey, true);
            } catch (IOException e) {
                System.out.println("No files uploaded yet");
            }
        } catch (IOException ioe) {
            System.err.println(localPeer.localAddress.getConnectAddress()
                    +  ": failed to connect to DHT pool");
        }
    }

    public void stabilise() {
        synchronized (this) {
            if (!stabilising && running) {
                stabilising = true;
                doStabilise();
                stabilising = false;
                return;
            }
        }
        while (running) {
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
        if (debug) System.out.println("stabilising... " + localPeer.localAddress.getConnectAddress());
        NeighbourState newState = new NeighbourState(localPeer.localAddress);
        Set<DhtPeerAddress> candidates = localPeer.getNeighbourState().getNeighbours();
        Set<DhtPeerAddress> asked = new HashSet<>();
        Set<DhtPeerAddress> failingPeers = new HashSet<>();

        if (candidates.size() == 0) {
            // disconnected, try reconnecting
            bootstrap();
            candidates = localPeer.getNeighbourState().getNeighbours();
        }

        while (!candidates.isEmpty()) {
            Set<DhtPeerAddress> newCandidates = new HashSet<>();

            for (DhtPeerAddress candidate : candidates ) {
                if (!asked.contains(candidate) &&
                        newState.isClose(candidate)) {
                    asked.add(candidate);
                    try {
                        NeighbourState remoteState =
                                localPeer.getClient().getNeighbourState(candidate);
                        newCandidates.addAll(remoteState.getNeighbours());
                        newState.addNeighbour(candidate);
                        if (debug) candidate.print(System.out, "Candidate responded: ");
                    } catch (IOException e) {
                        // candidate is failing, migrate responsibilities
                        failingPeers.add(candidate);
                        String msgText = "Failing link " +
                                localPeer.localAddress.getConnectAddress() + " - " +
                                candidate.getConnectAddress() + ": " + e.toString();
                        System.out.println(msgText);
                        if (debug)
                            System.err.println(msgText);
                    }
                }
            }
            candidates = newCandidates;
        }

        localPeer.setNeighbourState(newState);
        lastStabilised = System.nanoTime() / 1000000;
        if (debug) System.out.println("Updating fingers");
        localPeer.getFingerState().update();
        if (debug) System.out.println("Set new state");
        migrateResponsibilities(failingPeers);
        if (debug) System.out.println("Migrated responsibilities");
        stabiliseReplicas();
        if (debug) System.out.println("Stabilised replicas");
        localPeer.getDhtStore().vacuum();
        if (debug) System.out.println("Vacuumed DhtStore");
        localPeer.getClient().vacuumConnectionCace();
        if (debug) System.out.println("Vacuumed connection cache");

        if (debug) System.out.println("stabilised");
    }

    private void migrateResponsibilities(Set<DhtPeerAddress> failingPeers) {
        // set all foster file's responsibility to me for the time being
        // stabiliseReplicas will communicate with successors to establish who the real owner is
        for (DhtPeerAddress peer : failingPeers) {
            List<DhtFile> fosterFiles = localPeer.getDhtStore().getResponsibilitiesFor(peer);

            for (DhtFile file : fosterFiles)
            // assume we are responsible for all foster files
            // if this is not the case, stabilisation will take care
                localPeer.getDhtStore().refreshResponsibility(file.hash,
                        localPeer.localAddress, true);
        }
    }

    private void stabiliseReplicas() {
        List<DhtFile> myFiles = localPeer.getDhtStore().
                getResponsibilitiesFor(localPeer.localAddress);
        Set<DhtPeerAddress> neighbours = localPeer.getNeighbourState().getNeighbours();
        Set<BigInteger> doNotTransfer = new HashSet<>();
        HashMap<BigInteger, List<DhtPeerAddress>> transfers = new HashMap<>();

        // first talk to neighbours to determine who has what, what needs to be replicated
        for (DhtPeerAddress p : neighbours) {
            try {
                List<DhtFile> askFiles = new LinkedList<>();
                // calculate which files to enquire about
                for (DhtFile file : myFiles) {
                    // we are only interested in replicating if peer is a predecessor
                    // or if it is between us and the file
                    if (localPeer.getNeighbourState().isPredecessor(p) ||
                            p.isBetween(localPeer.localAddress,
                                    new DhtPeerAddress(file.hash, null, null,
                                            localPeer.localAddress.getUserID())))
                        askFiles.add(file);
                }

                // ask which files neighbour has
                Map<BigInteger, Boolean> stored = localPeer.getClient().storingFiles(p, askFiles);

                for (BigInteger file : stored.keySet()) {
                    if (!stored.get(file)) {
                        // add to transfers
                        if (!transfers.containsKey(file)) {
                            LinkedList<DhtPeerAddress> ll = new LinkedList<>();
                            ll.add(p);
                            transfers.put(file, ll);
                        } else
                            transfers.get(file).add(p);
                    } else if (localPeer.getDhtStore().refreshResponsibility(file, p, false))
                        // this means one of our successors has the file therefore
                        // we are wrong in believing that we are the owners
                        // hence prevent all transfers of this file
                        doNotTransfer.add(file);
                }
            } catch (IOException e) {
                // if a neighbour is not responding, the next synchronisation loop will take care
                if (debug) e.printStackTrace(System.out);
            }
        }

        // do all the transfers
        for (BigInteger file : transfers.keySet()) {
            if (!doNotTransfer.contains(file))
                for (DhtPeerAddress remotePeer : transfers.get(file)) {
                    try {
                        DirectTransfer ft = localPeer.getTransferManager().upload(
                            remotePeer, file, new InternalUploadContinuation(), false);
                        // track transfer with ft
                        // when transfer finishes, make it the new owner if is between me and file
                    } catch (IOException e) {
                        // no replication to failing link, the link will be deleted when
                        // we synchronize next
                        System.out.println("Failed auto-transferring " + file.toString() +
                                           " to " + remotePeer.getConnectAddress());
                    }
                }
        }
    }
}
