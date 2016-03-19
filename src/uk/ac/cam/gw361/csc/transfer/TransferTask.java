package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalPeer;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by gellert on 12/01/2016.
 */

public abstract class TransferTask {
    // indicates how many times to retry a transfer before giving up
    public static int maxRetries = 5;
    // how much to wait between retries
    public static int waitRetry = 3000;
    
    int retries = 0;
    abstract DirectTransfer execute() throws IOException;
}

class DownloadTask extends TransferTask {
    LocalPeer localPeer;
    String fileName;
    DhtFile file;
    boolean hashCheck;
    TransferContinuation continuation;
    boolean retry;

    DownloadTask(LocalPeer localPeer, String fileName, DhtFile file, boolean hashCheck,
                 TransferContinuation continuation, boolean retry) {
        this.localPeer = localPeer;
        this.fileName = fileName;
        this.file = file;
        this.hashCheck = hashCheck;
        this.continuation = continuation;
        this.retry = retry;
    }

    DirectTransfer execute() throws IOException {
        retries++;
        try {
            DirectTransfer transfer = localPeer.getClient().download(
                    fileName, file, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DirectTransfer retryExecute(IOException e) throws IOException {
        if (retry && retries < maxRetries) {
            System.out.println("Retrying " + fileName + " in " + waitRetry / 1000 + "s");
            try { Thread.sleep(waitRetry); } catch (InterruptedException ie) { }
            return execute();
        } else {
            System.out.println("Starting the execution of transfer " + file.hash.toString()
                    + " failed, giving up.");
            throw e;
        }
    }
}

class UploadTask extends TransferTask {
    LocalPeer localPeer;
    DhtPeerAddress target;
    BigInteger fileHash;
    TransferContinuation continuation;
    boolean retry;

    UploadTask(LocalPeer localPeer, DhtPeerAddress target, BigInteger fileHash,
                 TransferContinuation continuation, boolean retry) {
        this.localPeer = localPeer;
        this.target = target;
        this.fileHash = fileHash;
        this.continuation = continuation;
        this.retry = retry;
    }

    DirectTransfer execute() throws IOException {
        retries++;
        try {
            DirectTransfer transfer = localPeer.getClient().upload(
                    target, fileHash, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DirectTransfer retryExecute(IOException e) throws IOException {
        if (retry && retries < maxRetries) {
            System.out.println("Retrying " + fileHash.toString() + " in " + waitRetry / 1000 + "s");
            try { Thread.sleep(waitRetry); } catch (InterruptedException ie) { }
            return execute();
        } else {
            System.out.println("Starting the execution of transfer " + fileHash.toString()
                    + " failed, giving up.");
            throw e;
        }
    }
}

class NamedUploadTask extends TransferTask {
    LocalPeer localPeer;
    String name;
    FileUploadContinuation continuation;

    NamedUploadTask(LocalPeer localPeer, String name,
               FileUploadContinuation continuation) {
        this.localPeer = localPeer;
        this.name = name;
        this.continuation = continuation;
    }

    DirectTransfer execute() throws IOException {
        retries++;
        try {
            DirectTransfer transfer = localPeer.getClient().upload(
                    name, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DirectTransfer retryExecute(IOException e) throws IOException {
        if (retries < maxRetries) {
            System.out.println("Retrying " + name + " in " + waitRetry / 1000 + "s");
            try { Thread.sleep(waitRetry); } catch (InterruptedException ie) { }
            return execute();
        } else {
            System.out.println("Starting the execution of transfer " + name
                    + " failed, giving up.");
            throw e;
        }
    }
}

class SignedUploadTask extends TransferTask {
    LocalPeer localPeer;
    String name;
    BigInteger fileID;
    long timestamp;
    FileUploadContinuation continuation;

    SignedUploadTask(LocalPeer localPeer, String name, BigInteger fileID, long timestamp,
                     FileUploadContinuation continuation) {
        this.localPeer = localPeer;
        this.name = name;
        this.fileID = fileID;
        this.timestamp = timestamp;
        this.continuation = continuation;
    }

    DirectTransfer execute() throws IOException {
        retries++;
        try {
            DirectTransfer transfer = localPeer.getClient().signedUpload(
                    name, fileID, timestamp, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DirectTransfer retryExecute(IOException e) throws IOException {
        if (retries < maxRetries) {
            System.out.println("Retrying " + name + " in " + waitRetry / 1000 + "s");
            try { Thread.sleep(waitRetry); } catch (InterruptedException ie) { }
            return execute();
        } else {
            System.out.println("Starting the execution of transfer " + name
                    + " failed, giving up.");
            throw e;
        }
    }
}

