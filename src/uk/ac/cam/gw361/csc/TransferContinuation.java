package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by gellert on 24/12/2015.
 */
public abstract class TransferContinuation {
    abstract void notifyFinished(DirectTransfer finishedTransfer);
    
    // indicates how many times to retry a transfer before giving up
    int maxRetries = 3;
    // how much to wait between retries
    int waitRetry = 3000;
    
    DirectTransfer notifyFailed(DirectTransfer transfer) {
        // it is always the server's responsibility to handle failures
        // the standard reaction to client-mode failures is to retry
        // if we are in server-mode rather than client-mode, eg. we are asked to upload something
        // then there's no associated TransferTask in the originalTask

        DirectTransfer ret = null;
        BigInteger fileHash = transfer.transferFile.hash;

        if (transfer.originalTask != null && transfer.originalTask.retries < maxRetries) {
            System.out.println("Retrying transfer " + fileHash.toString() + " in " +
                    waitRetry / 1000 + "s");
            try { Thread.sleep(waitRetry); } catch (InterruptedException e) { }
            try { ret = transfer.originalTask.execute(); }
            catch (IOException e) {
                System.err.println("Giving up " + fileHash.toString() + " - " + e.toString());
            }
        } else {
            System.err.println("Giving up " + fileHash.toString());
        }
        return ret;
    }
}

class InternalUploadContinuation extends TransferContinuation {
    @Override
    public void notifyFinished(DirectTransfer finishedTransfer) {
        // upload complete, refresh responsibility for the file
        finishedTransfer.localPeer.getDhtStore().refreshResponsibility(
                finishedTransfer.transferFile.hash, finishedTransfer.remotePeer, false);
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer directTransfer) {
        // this is fine, we're either the client for the uploads
        // or if we are the server, stabiliser takes care of retries
        return null;
    }
}

class InternalDownloadContinuation extends TransferContinuation {
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

class FileUploadContinuation extends TransferContinuation {
    static String transferDir = "./uploads/";
    static int maxConcurrentTransfers = 5;
    int concurrentTransfers = 0;
    private boolean first = true;
    private int finishedBlocks = 0;
    private String lastName;
    private boolean fileListUpdated = false;
    private BigInteger metaHash;
    FileMetadata meta;
    TreeMap<Integer, BigInteger> waitingChunks;
    Set<DirectTransfer> runningTransfers = new HashSet<>();

    public static void createDir() {
        File myFolder = new File(transferDir);
        if (!myFolder.exists()) {
            System.out.println("creating directory: " + transferDir);
            if (!myFolder.mkdir())
                System.err.println("creating folder failed");
        }
    }

    FileUploadContinuation(String file, FileMetadata meta) throws IOException {
        this.meta = meta;
        waitingChunks = meta.getChunks();

        Path p = Paths.get(file);
        lastName = p.getFileName().toString();

        System.out.println("Copying file to upload directory");
        splitFile(file);
        System.out.println("Copying finished");
    }

    private void splitFile(String file) throws IOException {
        int index = 0;
        byte[] buffer = new byte[FileMetadata.blockSize];

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            int bytesRead = 0; // bytesRead should always be blockSize for internal blocks
            while ((bytesRead = bis.read(buffer)) > 0) {
                File newFile = new File(transferDir + lastName + "." + index++);
                try (BufferedOutputStream bos = new BufferedOutputStream(
                        new FileOutputStream(newFile))) {
                    bos.write(buffer, 0, bytesRead);
                }
            }
        }
    }

    private void removeFile(String file) {
        File f = new File(file);
        f.delete();
    }

    @Override
    public synchronized  void notifyFinished(DirectTransfer finishedTransfer) {
        LocalPeer localPeer = finishedTransfer.localPeer;
        runningTransfers.remove(finishedTransfer);
        if (first) {
            // metadata upload has finished, process metadata and start file blocks
            first = false;
            // save the hash of the metadata, the entry point for the file
            metaHash = finishedTransfer.transferFile.hash;
            System.out.println("Metadata uploaded");
        } else {
            if (concurrentTransfers > 0) {
                // excludes special uploads such as FileList
                finishedBlocks++;
                concurrentTransfers--;
                System.out.println("Uploaded " + finishedBlocks + "MB of " + meta.blocks + "MB");
            }

            // if done update FileList
            if (waitingChunks.isEmpty() && concurrentTransfers==0) {
                if (!fileListUpdated) {
                    System.out.println("File upload finished, updating FileList");
                    localPeer.fileList.put(lastName, metaHash);
                    String fileListPath = localPeer.saveFileList();
                    try {
                        BigInteger realHash = Hasher.hashFile(fileListPath);
                        runningTransfers.add(localPeer.getTransferManager().signedUpload(
                                fileListPath, localPeer.localAddress.getUserID(),
                                localPeer.fileList.getLastModified(), this));
                        fileListUpdated = true;
                    } catch (IOException e) {
                        System.err.println("File list file hashing problem:" + e.toString());
                    }
                } else {
                    System.out.println("Cleaning up");
                    removeFile(transferDir + lastName + ".meta");
                    for (int i=0; i<meta.blocks; i++)
                        removeFile(transferDir + lastName + "." + i);

                    System.out.println("Done.");
                }
            }
        }

        while (concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty()) {
            int nextIndex = waitingChunks.firstKey();
            waitingChunks.remove(waitingChunks.firstKey());

            concurrentTransfers++;
            try {
                runningTransfers.add(localPeer.getTransferManager().upload(
                        transferDir + lastName + "." + nextIndex, this));
            } catch (IOException e) {
                System.err.println("File upload failed");
            }
        }
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer transfer) {
        runningTransfers.remove(transfer);
        System.out.println("File chunk upload failed");
        DirectTransfer newTransfer = super.notifyFailed(transfer);
        if (newTransfer != null)
            runningTransfers.add(newTransfer);
        return newTransfer;
    }
}

class FileDownloadContinuation extends TransferContinuation {
    static String transferDir = "./downloads/";
    static int maxConcurrentTransfers = 5;
    int concurrentTransfers = 0;
    private boolean first = true;
    private int finishedBlocks = 0;
    private String fileName;
    FileMetadata meta;
    TreeMap<Integer, BigInteger> waitingChunks;
    Set<DirectTransfer> runningTransfers = new HashSet<>();

    public FileDownloadContinuation(String fileName) {
        this.fileName = fileName;
    }

    public static void createDir() {
        File myFolder = new File(transferDir);
        if (!myFolder.exists()) {
            System.out.println("creating directory: " + transferDir);
            if (!myFolder.mkdir())
                System.err.println("creating folder failed");
        }
    }

    private void mergeFile() throws IOException {
        int index = 0;
        byte[] buffer = new byte[FileMetadata.blockSize];

        try (FileOutputStream fos = new FileOutputStream(transferDir + fileName)) {
            try (BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                // opened target output file
                while ((new File(transferDir + fileName + "." + index)).exists()) {
                    // for each index if file exists open for input
                    try (BufferedInputStream bis = new BufferedInputStream(
                            new FileInputStream(transferDir + fileName + "." + index))) {
                        // read all contents of input chunk and write to output
                        int bytesRead;
                        while ((bytesRead = bis.read(buffer)) > 0) {
                            bos.write(buffer, 0, bytesRead);
                        }
                    }
                    bos.flush();

                    // delete file since it has been merged
                    new File(transferDir + fileName + "." + index).delete();

                    index++;
                }
            }
        }
    }

    @Override
    public synchronized  void notifyFinished(DirectTransfer finishedTransfer) {
        LocalPeer localPeer = finishedTransfer.localPeer;
        runningTransfers.remove(finishedTransfer);
        if (first) {
            // metadata download has finished, process metadata and start file blocks
            first = false;
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(transferDir + fileName + ".meta"))) {
                meta = (FileMetadata) ois.readObject();
                waitingChunks = meta.getChunks();
                System.out.println("Metadata collected");
                // delete metadata
                new File(transferDir + fileName + ".meta").delete();
            } catch (ClassNotFoundException | IOException e) {
                System.out.println("Error processing metadata: " + e.toString());
            }
        } else {
            finishedBlocks++;
            concurrentTransfers--;
            System.out.println("Downloaded " + finishedBlocks + "MB of " + meta.blocks + "MB");

            // if done merge downloaded chunks
            if (waitingChunks.isEmpty() && concurrentTransfers==0) {
                try {
                    mergeFile();
                    System.out.println("Merging succesful, download complete");
                } catch (IOException e) {
                    System.err.println("Merging failed: " + e.toString());
                }
            }
        }

        while (concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty()) {
            int nextIndex = waitingChunks.firstKey();
            BigInteger nextTransfer = waitingChunks.remove(waitingChunks.firstKey());

            concurrentTransfers++;
            try {
                runningTransfers.add(localPeer.getTransferManager().download(
                        FileDownloadContinuation.transferDir
                                + fileName + "." + nextIndex, nextTransfer, true, this));
            } catch (IOException e) {
                System.err.println("File download failed");
                e.printStackTrace();
            }
        }
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer transfer) {
        runningTransfers.remove(transfer);
        System.out.println("File chunk download failed");
        DirectTransfer newTransfer = super.notifyFailed(transfer);
        if (newTransfer != null)
            runningTransfers.add(newTransfer);
        return newTransfer;
    }
}

class FileListDownloadContinuation extends TransferContinuation {
    static String transferDir = "./downloads/";
    String fileName;
    PublicKey publicKey;

    FileListDownloadContinuation(String fileName, PublicKey publicKey) {
        this.fileName = fileName;
        this.publicKey = publicKey;
    }

    @Override
    public synchronized  void notifyFinished(DirectTransfer finishedTransfer) {
        if (!(finishedTransfer.transferFile instanceof SignedFile))
            System.err.println("FileListDownloadContinuation with a non-SignedFile download");

        LocalPeer localPeer = finishedTransfer.localPeer;
        FileList fileList = FileList.load(fileName, publicKey);
        if (fileList == null) {
            System.err.println("Error reading file list");
        } else {
            localPeer.setLastQueriedFileList(fileList);
        }
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer transfer) {
        System.out.println("File list download failed");
        return super.notifyFailed(transfer);
    }
}
