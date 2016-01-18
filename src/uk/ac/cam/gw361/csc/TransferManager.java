package uk.ac.cam.gw361.csc;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by gellert on 18/01/2016.
 */
public class TransferManager {
    // The purpose of this class is to wrap DhtClient operations with TransferTasks which
    // take care of retries if failures happen.
    // In particular, a TransferTask can repeat the entire task by calling execute.

    LocalPeer localPeer;
    TransferManager(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    DhtTransfer download(String fileName, BigInteger fileHash, boolean hashCheck,
                         TransferContinuation continuation) throws IOException {
        TransferTask task = new DownloadTask(localPeer, fileName, fileHash, hashCheck, continuation);
        return task.execute();
    }

    DhtTransfer upload(DhtPeerAddress target, BigInteger file, DhtPeerAddress owner,
                       TransferContinuation continuation) throws IOException {
        TransferTask task = new UploadTask(localPeer, target, file, owner, continuation, true);
        return task.execute();
    }

    DhtTransfer uploadNoRetry(DhtPeerAddress target, BigInteger file, DhtPeerAddress owner,
                       TransferContinuation continuation) throws IOException {
        TransferTask task = new UploadTask(localPeer, target, file, owner, continuation, false);
        return task.execute();
    }

    DhtTransfer upload(String name, FileUploadContinuation continuation) throws IOException {
        TransferTask task = new NamedUploadTask(localPeer, name, continuation);
        return task.execute();
    }

    public DhtTransfer signedUpload(String name, BigInteger fileID, BigInteger realHash,
                                    FileUploadContinuation continuation) throws IOException {
        TransferTask task = new SignedUploadTask(localPeer, name, fileID, realHash, continuation);
        return task.execute();
    }
}
