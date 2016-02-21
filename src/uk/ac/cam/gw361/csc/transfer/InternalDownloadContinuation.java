package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.dht.LocalPeer;

import java.math.BigInteger;

/**
 * Created by gellert on 21/02/2016.
 */
public class InternalDownloadContinuation extends TransferContinuation {
    @Override
    public void notifyFinished(DirectTransfer finishedTransfer) {
        // complete download, add to list of local files
        LocalPeer localPeer = finishedTransfer.localPeer;
        if (finishedTransfer.transferFile.owner != null) {
            // add
            localPeer.getDhtStore().addFile(finishedTransfer.transferFile);

            // maybe we are the next owners
            BigInteger fileHash = finishedTransfer.transferFile.hash;
            localPeer.getDhtStore().refreshResponsibility(fileHash,
                    localPeer.localAddress, false);

            // replication to other peers will be handled by the Stabiliser
        }
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer directTransfer) {
        // this is fine, we're always in server mode
        return null;
    }
}
