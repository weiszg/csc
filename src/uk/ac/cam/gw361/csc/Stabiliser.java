package uk.ac.cam.gw361.csc;

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
            stabilise();
        }
    }

    public void stabilise() {
        synchronized (this) {
            if (!stabilising && running) {
                doStabilise();
                return;
            }
        }
        while (true && running) {
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

        if (debug) System.out.println("stabilising... " + localPeer.localAddress.getPort());
        NeighbourState newState = new NeighbourState(localPeer.localAddress);
        Set<DhtPeerAddress> candidates = localPeer.getNeighbourState().getNeighbours();
        Set<DhtPeerAddress> asked = new HashSet<>();
        Set<DhtPeerAddress> failingPeers = new HashSet<>();

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
                    } catch (IOException e) {
                        // candidate is failing, migrate responsibilities
                        failingPeers.add(candidate);
                        System.err.println("Failing link " + localPeer.localAddress.getHost() + ":" +
                                localPeer.localAddress.getPort() + " - " + candidate.getHost() + ":" +
                                candidate.getPort());
                    }
                }
            }
            candidates = newCandidates;
        }

        localPeer.setNeighbourState(newState);
        migrateResponsibilities(failingPeers);
        stabiliseReplicas();
        localPeer.getDhtStore().vacuum();

        if (debug) System.out.println("stabilised");
        synchronized (this) {
            stabilising = false;
        }
    }

    private void migrateResponsibilities(Set<DhtPeerAddress> failingPeers) {
        // set all foster file's responsibility to me for the time being
        // stabiliseReplicas will communicate with successors to establish who the real owner is
        for (DhtPeerAddress peer : failingPeers) {
            List<DhtFile> fosterFiles = localPeer.getDhtStore().getResponsibilitiesFor(peer);
            for (DhtFile file : fosterFiles)
                localPeer.getDhtStore().refreshResponsibility(file.fileHash,
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
                for (DhtFile file : myFiles)
                    // we are only interested in replicating if peer is a predecessor
                    // or if it is between us and the file
                    if (localPeer.getNeighbourState().isPredecessor(p) ||
                            p.isBetween(localPeer.localAddress,
                                    new DhtPeerAddress(file.fileHash, null, null,
                                            localPeer.localAddress.getUserID())))
                        askFiles.add(file);

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
                if (debug) e.printStackTrace();
            }
        }

        // do all the transfers
        for (BigInteger file : transfers.keySet()) {
            if (!doNotTransfer.contains(file))
                for (DhtPeerAddress remotePeer : transfers.get(file)) {
                    try {
                        DhtTransfer ft = localPeer.getClient().upload(remotePeer, file,
                                localPeer.localAddress, null);
                        localPeer.addRunningTransfer(ft);
                        // when transfer finishes, make it the new owner if is between me and file
                    } catch (IOException e) {
                        // no replication to failing link, the link will be deleted when
                        // we synchronize next
                        e.printStackTrace();
                    }
                }
        }
    }
}
