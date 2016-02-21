package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.storage.FileList;
import uk.ac.cam.gw361.csc.storage.SignedFile;

import java.security.PublicKey;

/**
 * Created by gellert on 21/02/2016.
 */
public class FileListDownloadContinuation extends TransferContinuation {
    public static String transferDir = "./downloads/";
    String fileName;
    PublicKey publicKey;
    boolean own;

    public FileListDownloadContinuation(String fileName, PublicKey publicKey, boolean own) {
        this.fileName = fileName;
        this.publicKey = publicKey;
    }

    @Override
    public synchronized  void notifyFinished(DirectTransfer finishedTransfer) {
        if (!(finishedTransfer.transferFile instanceof SignedFile))
            System.err.println("FileListDownloadContinuation with a non-SignedFile download");

        LocalPeer localPeer = finishedTransfer.localPeer;
        FileList fileList = FileList.load(fileName + ".signed", publicKey);
        if (fileList == null) {
            System.err.println("Error reading file list");
        } else {
            if (own)
                localPeer.fileList = fileList;
            localPeer.setLastQueriedFileList(fileList);
        }
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer transfer) {
        System.out.println("File list download failed");
        return super.notifyFailed(transfer);
    }
}
