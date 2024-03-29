package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.storage.DhtFile;
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
    private final HashMap<BigInteger, Integer> replicationDegree = new HashMap<>();
    private boolean bootstrapped = false;

    boolean isStable() {
        return (System.nanoTime() / 1000000 - lastStabilised <= interval * 2);
    }

    long millisSinceStabilised() {
        return System.nanoTime() / 1000000 - lastStabilised;
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
                // try fetching my file list if I'm a client
                if (localPeer.isCscOnly())
                    localPeer.getFileList(localPeer.userName, localPeer.publicKey, true);
            } catch (IOException e) {
                System.out.println("No files uploaded yet");
            }
        } catch (ConnectionFailedException e) {
            System.err.println(localPeer.localAddress.getConnectAddress()
                    +  ": failed to connect to DHT pool at " + bootstrapPeer + ": " + e.reason);
        } catch (IOException ioe) {
            System.err.println(localPeer.localAddress.getConnectAddress()
                +  ": failed to connect to DHT pool at " + bootstrapPeer + ": " + ioe.toString());
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
        HashSet<DhtPeerAddress> failingPeers = new HashSet<>();

        if (candidates.size() == 0 || !bootstrapped) {
            // disconnected, try reconnecting
            bootstrapped = true;
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
        migrateResponsibilities();
        if (debug) System.out.println("Migrated responsibilities");
        stabiliseReplicas();
        if (debug) System.out.println("Stabilised replicas");
        localPeer.getDhtStore().vacuum();
        if (debug) System.out.println("Vacuumed DhtStore");
        localPeer.getClient().vacuumConnectionCache();
        if (debug) System.out.println("Vacuumed connection cache");

        if (debug) System.out.println("stabilised");
    }

    private void migrateResponsibilities() {
        // set all responsibilities to the neighbour that is closest and still available
        // stabiliseReplicas will resolve these hypotheses with them

        for (Map.Entry<BigInteger, DhtFile> file :
                localPeer.getDhtStore().getStoredFiles().entrySet()) {
            // find the neighbour most responsible for this file
            // we might be wrong but stabilisation will take care
            DhtPeerAddress newOwner = localPeer.getNextLocalHop(file.getKey()).neighbour;
            file.getValue().owner = newOwner;
        }
    }

    private void stabiliseReplicas() {
        HashMap<BigInteger, DhtFile> storedFiles = localPeer.getDhtStore().getStoredFiles();
        TreeSet<DhtPeerAddress> neighbours = localPeer.getNeighbourState().getNeighbours();
        DhtPeerAddress immediateSuccessor = localPeer.getNeighbourState().getImmediateSuccessor();
        Set<BigInteger> doNotTransfer = new HashSet<>();
        TreeSet<ReplicationTask> transfers = new TreeSet<>();

        synchronized (replicationDegree) {
            // reset replication degree
            replicationDegree.clear();

            // first talk to neighbours to determine who has what, what needs to be replicated
            for (DhtPeerAddress p : neighbours) {
                // establish priority
                int priority;
                if (localPeer.getNeighbourState().isPredecessor(p)) {
                    priority = localPeer.getNeighbourState().getDistance(p);
                    if (priority > 1) priority++;
                } else {
                    priority = 2;
                }

                try {
                    List<DhtFile> askFiles = new LinkedList<>();
                    // calculate which files to enquire about
                    for (DhtFile file : storedFiles.values()) {
                        if (file.owner.equals(localPeer.localAddress)) {
                            // we are RESPONSIBLE for this file!
                            // we are only interested in replicating if peer is a predecessor
                            // or if it is between us and the file
                            if (localPeer.getNeighbourState().isPredecessor(p) ||
                                    p.isBetween(localPeer.localAddress,
                                            new DhtPeerAddress(file.hash, null, null,
                                                    localPeer.localAddress.getUserID()))) {
                                if (debug)
                                    System.out.println("c) " + localPeer.localAddress.getConnectAddress() + "" +
                                        " ask " + p.getConnectAddress() + " about " +
                                        file.hash.toString());

                                askFiles.add(file);
                            }
                        } else if (p.equals(immediateSuccessor) || p.equals(file.owner)) {
                            // we are not responsible for this file
                            // however, we're talking to p, the immediate successor and either
                            // a) p >= hash of file
                            //     in which case we are wrong and should be responsible
                            // b) or p too has to replicate the file but might not
                            //    if the owner is overwhelmed
                            // let's make sure!
                            if (p.isBetween(localPeer.localAddress,
                                    new DhtPeerAddress(file.hash, null, null,
                                            localPeer.localAddress.getUserID()))) {
                                // scenario b)
                                if (debug)
                                    System.out.println("b) " + localPeer.localAddress.getConnectAddress() + "" +
                                        " ask " + p.getConnectAddress() + " about " +
                                        file.hash.toString());

                                askFiles.add(file);
                            } else {
                                // scenario a)
                                if (debug)
                                    System.out.println("a)");

                                localPeer.getDhtStore().refreshResponsibility(
                                        file.hash, localPeer.localAddress, true);
                            }
                        }
                    }

                    // ask which files neighbour has
                    Map<BigInteger, Boolean> stored = localPeer.getClient().storingFiles(p, askFiles);

                    for (BigInteger file : stored.keySet()) {
                        if (!stored.get(file)) {
                            // add to transfers
                            transfers.add(new ReplicationTask(priority, file, p));

                        } else if (storedFiles.get(file).owner.equals(localPeer.localAddress)) {
                            // increase degree of replication
                            Integer degree = replicationDegree.get(file);
                            if (degree == null) degree = 0;
                            replicationDegree.put(file, degree + 1);

                            if (localPeer.getDhtStore().refreshResponsibility(file, p, false))
                                // this means one of our successors has the file therefore
                                // we are wrong in believing that we are the owners
                                // hence prevent all transfers of this file and refresh responsibility
                                doNotTransfer.add(file);
                        }
                    }
                } catch (IOException e) {
                    // if a neighbour is not responding, the next synchronisation loop will take care
                    if (debug) e.printStackTrace(System.out);
                }
            }
        }

        // do all the transfers
        for (ReplicationTask task : transfers) {
            if (!doNotTransfer.contains(task.file)) {
                try {
                    localPeer.getTransferManager().upload(
                            task.target, task.file,
                            new InternalUploadContinuation(), false);
                    // when transfer finishes, make it the new owner if is between me and file
                } catch (IOException e) {
                    // no replication to failing link, the link will be deleted when
                    // we synchronize next
                    System.out.println("Failed auto-transferring " + task.file.toString() +
                            " to " + task.target.getConnectAddress());
                }
            }
        }
    }

    HashMap<BigInteger, Integer> getReplicationDegree() {
        // returns the degree of replication for each file owned, -1 if unknown
        HashMap<BigInteger, Integer> replications = new HashMap<>();
        synchronized (replicationDegree) {
            List<DhtFile> myFiles = localPeer.getDhtStore().
                    getResponsibilitiesFor(localPeer.localAddress);
            for (DhtFile file : myFiles) {
                Integer degree = replications.get(file.hash);
                if (degree == null) degree = -1;
                replications.put(file.hash, degree);
            }
        }
        return replications;
    }

}

class ReplicationTask implements Comparable<ReplicationTask> {
    int priority;
    BigInteger file;
    DhtPeerAddress target;

    ReplicationTask(int priority, BigInteger file, DhtPeerAddress target) {
        this.priority = priority;
        this.target = target;
        this.file = file;
    }

    @Override
    public int compareTo(ReplicationTask other) {
        if (priority == other.priority) {
            return (file.hashCode() + target.getUserID().hashCode()
                    - other.file.hashCode() - other.target.getUserID().hashCode());
        } else
            return priority - other.priority;
    }
}
