package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.storage.FileMetadata;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;

/**
 * Created by gellert on 21/02/2016.
 */
public class FileUploadContinuation extends TransferContinuation {
    public static String transferDir = "./uploads/";
    static int maxConcurrentTransfers = 5;
    int concurrentTransfers = 0;
    private boolean first = true;
    private int finishedBlocks = 0;
    private String fileName;
    private String lastName;
    private boolean fileListUpdated = false;
    private BigInteger metaHash;
    FileMetadata meta;
    TreeMap<Integer, BigInteger> waitingChunks;

    public static void createDir() {
        File myFolder = new File(transferDir);
        if (!myFolder.exists()) {
            System.out.println("creating directory: " + transferDir);
            if (!myFolder.mkdir())
                System.err.println("creating folder failed");
        }
    }

    public FileUploadContinuation(String file, FileMetadata meta) throws IOException {
        this.meta = meta;
        this.fileName = file;
        waitingChunks = meta.getChunks();

        Path p = Paths.get(file);
        lastName = p.getFileName().toString();

        System.out.println("Copying file to upload directory");
        splitFile(file, meta.blockSize);
        System.out.println("Copying finished");
    }

    private void splitFile(String file, int blockSize) throws IOException {
        int index = 0;
        byte[] buffer = new byte[blockSize];

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

    private synchronized boolean canStartMore() {
        return concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty();
    }

    @Override
    public  void notifyFinished(DirectTransfer finishedTransfer) {
        LocalPeer localPeer = finishedTransfer.localPeer;
        synchronized (this) {
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
                    System.out.println("Uploaded " + finishedBlocks + " chunks, " +
                            finishedBlocks * meta.blockSize / (1024*1024) + "MB of "
                            + meta.blocks * meta.blockSize / (1024*1024) + "MB, threads: "
                            + concurrentTransfers);
                    concurrentTransfers--;
                }

                // if done update FileList
                if (waitingChunks.isEmpty() && concurrentTransfers == 0) {
                    if (!fileListUpdated) {
                        System.out.println("File upload finished, updating FileList");
                        localPeer.fileList.put(lastName, metaHash);
                        String fileListPath = localPeer.saveFileList();
                        try {
                            localPeer.getTransferManager().signedUpload(
                                    fileListPath, localPeer.localAddress.getUserID(),
                                    localPeer.fileList.getLastModified(), this);
                            fileListUpdated = true;
                        } catch (IOException e) {
                            System.err.println("File list upload problem:" + e.toString());
                        }
                    } else {
                        System.out.println("Cleaning up");
                        removeFile(transferDir + lastName + ".meta");
                        for (int i = 0; i < meta.blocks; i++)
                            removeFile(transferDir + lastName + "." + i);

                        System.out.println("Done.");
                        localPeer.notifyDone(fileName);
                    }
                }
            }
        }

        while (canStartMore()) {
            int nextIndex;
            synchronized (this) {
                nextIndex = waitingChunks.firstKey();
                waitingChunks.remove(waitingChunks.firstKey());
                concurrentTransfers++;
            }

            try {
                localPeer.getTransferManager().upload(
                        transferDir + lastName + "." + nextIndex, this);
            } catch (IOException e) {
                synchronized (this) { concurrentTransfers--; }
                System.err.println("File upload failed");
            }
        }
    }

    @Override
    public void notifyFailed(DirectTransfer transfer) {
        System.out.println("File chunk upload failed");
        super.notifyFailed(transfer);
    }
}
