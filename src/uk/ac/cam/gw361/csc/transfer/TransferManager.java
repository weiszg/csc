package uk.ac.cam.gw361.csc.transfer;

import com.sun.istack.internal.NotNull;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.storage.SignedFile;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeSet;

/**
 * Created by gellert on 18/01/2016.
 */
public class TransferManager extends Thread {
    // The purpose of this class is to wrap DhtClient operations with TransferTasks which
    // take care of retries if failures happen.
    // In particular, a TransferTask can repeat the entire task by calling execute.

    LocalPeer localPeer;
    final TreeSet<TransferRequest> requests = new TreeSet<>();
    final LinkedList<DirectTransfer> running = new LinkedList<>();
    //final HashSet<BigInteger> filesInProgress = new HashSet<>();
    int maxConcurrentTransfers = 20;
    int maxQueueLength = 200;
    boolean offloadEnabled = false;
    boolean debug = false;
    long queueFullCt = 0;


    public TransferManager(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void notifyTransferCompleted(DirectTransfer ft, boolean success) {
        // server-mode transfers aren't tracked here so it's fine if running doesn't have ft
        synchronized (requests) {
            running.remove(ft);
            requests.notify();
            if (debug) System.out.println("notifyTransferCompleted " +
                    requests.size() + " " + running.size());
        }
    }

    private void addRequest(TransferTask task) throws IOException {
        synchronized (requests) {
            if (requests.size() > maxQueueLength) {
                if (queueFullCt % 100 == 0)
                    System.err.println("Refusing request: queue full");
                queueFullCt++;
            } else if (offloadEnabled &&
                    running.size() < maxConcurrentTransfers) {
                // execute this right away
                DirectTransfer transfer = task.execute();
                if (transfer != null && !transfer.stopped)
                    running.add(transfer);
            } else {
                requests.add(new TransferRequest(task, System.currentTimeMillis()));
                requests.notify();
            }
            if (debug) System.out.println("addRequest " + requests.size() + " " + running.size() +
             " " + task.hashCode());
        }
    }

    public void queueTask(TransferTask task, int wait) {
        synchronized (requests) {
            requests.add(new TransferRequest(task, System.currentTimeMillis() + wait));
            if (debug) System.out.println("queueTask " + requests.size() + " " + running.size());
        }
    }

    public void run() {
        while (true) {
            TransferTask task = null;
            synchronized (requests) {
                if (running.size() < maxConcurrentTransfers && !requests.isEmpty()) {
                    // execute the top transfer
                    if (requests.first().timeStart > System.currentTimeMillis())
                        try { requests.wait(500); } catch (InterruptedException e) { }
                    else {
                        task = requests.pollFirst().task;
                        if (debug) System.out.println("executing request " +
                                requests.size() + " " + running.size());
                    }
                } else {
                    Long timeBefore = System.currentTimeMillis();
                    try { requests.wait(11000); } catch (InterruptedException e) { }
                    Long timeAfter = System.currentTimeMillis();
                    if (timeAfter - timeBefore > 10000 && !running.isEmpty())
                        if (debug) System.err.println("Transfer starving for " +
                                localPeer.localAddress.getConnectAddress());
                }
            }
            if (task != null)
                try {
                    DirectTransfer transfer = task.execute();
                    if (transfer != null && !transfer.stopped)
                        synchronized (requests) {
                            running.add(transfer);
                        }
                } catch (IOException e) {
                    System.err.println("Error executing transfer: " + e.toString());
                }
            }
        }

    public void download(String fileName, BigInteger fileHash, boolean hashCheck,
                         TransferContinuation continuation, boolean retry) throws IOException {
        DhtFile toDownload;
        if (hashCheck)
            toDownload = new DhtFile(fileHash, null, null);
        else
            toDownload = new SignedFile(fileHash, null, null, null);

        TransferTask task = new DownloadTask(localPeer, fileName, toDownload, hashCheck,
                continuation, retry);

        addRequest(task);
    }

    public void upload(DhtPeerAddress target, BigInteger file,
                       TransferContinuation continuation, boolean retry) throws IOException {
        TransferTask task = new UploadTask(localPeer, target, file, continuation, retry);
        addRequest(task);
    }

    public void upload(String name, FileUploadContinuation continuation) throws IOException {
        TransferTask task = new NamedUploadTask(localPeer, name, continuation);
        addRequest(task);
    }

    public void signedUpload(String name, BigInteger fileID, long timestamp,
                                    FileUploadContinuation continuation) throws IOException {
        TransferTask task = new SignedUploadTask(localPeer, name, fileID, timestamp, continuation);
        addRequest(task);
    }
}

class TransferRequest implements Comparable<TransferRequest> {
    TransferTask task;
    Long timeStart;
    Long ticketNumber;
    static long totalTickets = 0;

    TransferRequest(TransferTask task, long timeStart) {
        this.task = task;
        this.timeStart = timeStart;
        synchronized (this) {
            ticketNumber = totalTickets++;
        }
    }

    @Override
    public int compareTo(@NotNull TransferRequest other) {
        int timeCompare = timeStart.compareTo(other.timeStart);
        if (timeCompare != 0)
            return timeCompare;
        else {
            return ticketNumber.compareTo(other.ticketNumber);
        }
    }
}