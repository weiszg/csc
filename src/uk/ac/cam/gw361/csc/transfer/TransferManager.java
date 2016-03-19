package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.storage.SignedFile;

import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedList;

/**
 * Created by gellert on 18/01/2016.
 */
public class TransferManager extends Thread {
    // The purpose of this class is to wrap DhtClient operations with TransferTasks which
    // take care of retries if failures happen.
    // In particular, a TransferTask can repeat the entire task by calling execute.

    LocalPeer localPeer;
    final LinkedList<TransferTask> requests = new LinkedList<>();
    final LinkedList<DirectTransfer> running = new LinkedList<>();
    int maxConcurrentTransfers = 10;

    public TransferManager(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void notifyTransferCompleted(DirectTransfer ft, boolean success) {
        synchronized (requests) {
            running.remove(ft);
            requests.notify();
        }
    }

    private void addRequest(TransferTask task) {
        synchronized (requests) {
            requests.add(task);
            requests.notify();
        }
    }

    public void run() {
        synchronized (requests) {
            while (true) {
                if (running.size() < maxConcurrentTransfers && !requests.isEmpty()) {
                    // execute the top transfer
                    TransferTask task = requests.pollFirst();
                    try {
                        DirectTransfer transfer = task.execute();
                        if (!transfer.stopped)
                            running.add(transfer);
                    } catch (IOException e) {
                        System.err.println("Error executing transfer: " + e.toString());
                    }
                } else {
                    Long timeBefore = System.currentTimeMillis();
                    try { requests.wait(11000); } catch (InterruptedException e) { }
                    Long timeAfter = System.currentTimeMillis();
                    if (timeAfter - timeBefore > 10000 && !running.isEmpty())
                        System.err.println("Transfer starving for " +
                                localPeer.localAddress.getConnectAddress());
                }
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
