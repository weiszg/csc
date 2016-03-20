package uk.ac.cam.gw361.csc.transfer;

import java.io.*;
import java.math.BigInteger;
/**
 * Created by gellert on 24/12/2015.
 */
public abstract class TransferContinuation {
    abstract void notifyFinished(DirectTransfer finishedTransfer);
    
    // indicates how many times to retry a transfer before giving up
    int maxRetries = 100;
    // how much to wait between retries
    int waitRetry = 3000;
    
    void notifyFailed(DirectTransfer transfer) {
        // it is always the server's responsibility to handle failures
        // the standard reaction to client-mode failures is to retry
        // if we are in server-mode rather than client-mode, eg. we are asked to upload something
        // then there's no associated TransferTask in the originalTask

        BigInteger fileHash = transfer.transferFile.hash;

        if (transfer.originalTask != null && transfer.originalTask.retries < maxRetries) {
            System.out.println("Retrying transfer " + fileHash.toString() + " in " +
                    waitRetry / 1000 + "s");

            transfer.localPeer.getTransferManager().queueTask(transfer.originalTask, waitRetry);
        } else if (transfer.originalTask != null) {
            System.err.println("Giving up " + fileHash.toString());
        }
    }
}
