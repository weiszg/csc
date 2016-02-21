package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.storage.SignedFile;

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
    public TransferManager(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public DirectTransfer download(String fileName, BigInteger fileHash, boolean hashCheck,
                         TransferContinuation continuation, boolean retry) throws IOException {
        DhtFile toDownload;
        if (hashCheck)
            toDownload = new DhtFile(fileHash, null, null);
        else
            toDownload = new SignedFile(fileHash, null, null, null);

        TransferTask task = new DownloadTask(localPeer, fileName, toDownload, hashCheck,
                continuation, retry);
        return task.execute();
    }

    public DirectTransfer upload(DhtPeerAddress target, BigInteger file,
                       TransferContinuation continuation, boolean retry) throws IOException {
        TransferTask task = new UploadTask(localPeer, target, file, continuation, retry);
        return task.execute();
    }

    public DirectTransfer upload(String name, FileUploadContinuation continuation) throws IOException {
        TransferTask task = new NamedUploadTask(localPeer, name, continuation);
        return task.execute();
    }

    public DirectTransfer signedUpload(String name, BigInteger fileID, long timestamp,
                                    FileUploadContinuation continuation) throws IOException {
        TransferTask task = new SignedUploadTask(localPeer, name, fileID, timestamp, continuation);
        return task.execute();
    }
}
