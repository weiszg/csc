package uk.ac.cam.gw361.csc.transfer;

/**
 * Created by gellert on 21/02/2016.
 */

public class InternalUploadContinuation extends TransferContinuation {
    @Override
    public void notifyFinished(DirectTransfer finishedTransfer) {
        // upload complete, refresh responsibility for the file

        // actually, wait until they stabilise with you for this
        //finishedTransfer.localPeer.getDhtStore().refreshResponsibility(
        //        finishedTransfer.transferFile.hash, finishedTransfer.remotePeer, false);
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer directTransfer) {
        // this is fine, we're either the client for the uploads
        // or if we are the server, stabiliser takes care of retries
        return null;
    }
}
