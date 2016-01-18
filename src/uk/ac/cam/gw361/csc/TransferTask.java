package uk.ac.cam.gw361.csc;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by gellert on 12/01/2016.
 */

public abstract class TransferTask {
    // indicates how many times to retry a transfer before giving up
    int maxRetries = 3;
    // how much to wait between retries
    int waitRetry = 3000;
    
    int retries = 0;
    abstract DhtTransfer execute() throws IOException;
}

class DownloadTask extends TransferTask {
    LocalPeer localPeer;
    String fileName;
    BigInteger fileHash;
    boolean hashCheck;
    TransferContinuation continuation;

    DownloadTask(LocalPeer localPeer, String fileName, BigInteger fileHash, boolean hashCheck,
                 TransferContinuation continuation) {
        this.localPeer = localPeer;
        this.fileName = fileName;
        this.fileHash = fileHash;
        this.hashCheck = hashCheck;
        this.continuation = continuation;
    }

    DhtTransfer execute() throws IOException {
        retries++;
        try {
            DhtTransfer transfer = localPeer.getClient().download(
                    fileName, fileHash, hashCheck, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DhtTransfer retryExecute(IOException e) throws IOException {
        if (retries < maxRetries) {
            System.out.println("Starting the execution of transfer " + fileHash.toString()
                    + " failed, retrying transfer in " + waitRetry / 1000 + "s");
            try { Thread.sleep(waitRetry); } catch (InterruptedException ie) { }
            return execute();
        } else {
            System.out.println("Starting the execution of transfer " + fileHash.toString()
                    + " failed, giving up.");
            throw e;
        }
    }
}

class UploadTask extends TransferTask {
    LocalPeer localPeer;
    DhtPeerAddress target;
    BigInteger fileHash;
    DhtPeerAddress owner;
    TransferContinuation continuation;
    boolean retry;

    UploadTask(LocalPeer localPeer, DhtPeerAddress target, BigInteger fileHash, DhtPeerAddress owner,
                 TransferContinuation continuation, boolean retry) {
        this.localPeer = localPeer;
        this.target = target;
        this.fileHash = fileHash;
        this.owner = owner;
        this.continuation = continuation;
        this.retry = retry;
    }

    DhtTransfer execute() throws IOException {
        retries++;
        try {
            DhtTransfer transfer = localPeer.getClient().upload(
                    target, fileHash, owner, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DhtTransfer retryExecute(IOException e) throws IOException {
        if (retry && retries < maxRetries) {
            System.out.println("Starting the execution of transfer " + fileHash.toString()
                    + " failed, retrying transfer in " + waitRetry / 1000 + "s");
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

    DhtTransfer execute() throws IOException {
        retries++;
        try {
            DhtTransfer transfer = localPeer.getClient().upload(
                    name, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DhtTransfer retryExecute(IOException e) throws IOException {
        if (retries < maxRetries) {
            System.out.println("Starting the execution of transfer " + name
                    + " failed, retrying transfer in " + waitRetry / 1000 + "s");
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
    BigInteger fileID; BigInteger realHash;
    FileUploadContinuation continuation;

    SignedUploadTask(LocalPeer localPeer, String name, BigInteger fileID, BigInteger realHash,
                     FileUploadContinuation continuation) {
        this.localPeer = localPeer;
        this.name = name;
        this.fileID = fileID;
        this.realHash = realHash;
        this.continuation = continuation;
    }

    DhtTransfer execute() throws IOException {
        retries++;
        try {
            DhtTransfer transfer = localPeer.getClient().signedUpload(
                    name, fileID, realHash, continuation);
            transfer.setOriginalTask(this);
            return transfer;
        } catch (IOException e) {
            System.out.println("Error executing DownloadTask: " + e.toString());
            return retryExecute(e);
        }
    }

    private DhtTransfer retryExecute(IOException e) throws IOException {
        if (retries < maxRetries) {
            System.out.println("Starting the execution of transfer " + name
                    + " failed, retrying transfer in " + waitRetry / 1000 + "s");
            try { Thread.sleep(waitRetry); } catch (InterruptedException ie) { }
            return execute();
        } else {
            System.out.println("Starting the execution of transfer " + name
                    + " failed, giving up.");
            throw e;
        }
    }
}

