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

    DirectTransfer download(String fileName, BigInteger fileHash, boolean hashCheck,
                         TransferContinuation continuation) throws IOException {
        DhtFile toDownload;
        if (hashCheck)
            toDownload = new DhtFile(fileHash, null, null);
        else
            toDownload = new SignedFile(fileHash, null, null, null);

        TransferTask task = new DownloadTask(localPeer, fileName, toDownload, hashCheck, continuation);
        return task.execute();
    }

    DirectTransfer upload(DhtPeerAddress target, BigInteger file,
                       TransferContinuation continuation) throws IOException {
        TransferTask task = new UploadTask(localPeer, target, file, continuation, true);
        return task.execute();
    }

    DirectTransfer uploadNoRetry(DhtPeerAddress target, BigInteger file,
                       TransferContinuation continuation) throws IOException {
        TransferTask task = new UploadTask(localPeer, target, file, continuation, false);
        return task.execute();
    }

    DirectTransfer upload(String name, FileUploadContinuation continuation) throws IOException {
        TransferTask task = new NamedUploadTask(localPeer, name, continuation);
        return task.execute();
    }

    public DirectTransfer signedUpload(String name, BigInteger fileID, long timestamp,
                                    FileUploadContinuation continuation) throws IOException {
        TransferTask task = new SignedUploadTask(localPeer, name, fileID, timestamp, continuation);
        return task.execute();
    }
}
