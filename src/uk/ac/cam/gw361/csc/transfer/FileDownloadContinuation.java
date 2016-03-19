package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.storage.FileMetadata;

import java.io.*;
import java.math.BigInteger;
import java.util.TreeMap;

/**
 * Created by gellert on 21/02/2016.
 */
public class FileDownloadContinuation extends TransferContinuation {
    public static String transferDir = "./downloads/";
    static int maxConcurrentTransfers = 5;
    int concurrentTransfers = 0;
    private boolean first = true;
    private int finishedBlocks = 0;
    private String fileName;
    FileMetadata meta;
    TreeMap<Integer, BigInteger> waitingChunks;

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

    private void mergeFile(int blockSize) throws IOException {
        int index = 0;
        byte[] buffer = new byte[blockSize];

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

    private synchronized boolean canStartMore() {
        return concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty();
    }

    @Override
    public void notifyFinished(DirectTransfer finishedTransfer) {
        LocalPeer localPeer = finishedTransfer.localPeer;
        synchronized (this) {
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
                System.out.println("Downloaded " + finishedBlocks + " chunks, " +
                        finishedBlocks * meta.blockSize / (1024*1024) + "MB of "
                        + meta.blocks * meta.blockSize / (1024*1024) + "MB, threads: "
                        + concurrentTransfers);
                concurrentTransfers--;

                // if done merge downloaded chunks
                if (waitingChunks.isEmpty() && concurrentTransfers == 0) {
                    try {
                        mergeFile(meta.blockSize);
                        System.out.println("Merging succesful, download complete");
                    } catch (IOException e) {
                        System.err.println("Merging failed: " + e.toString());
                    }
                }
            }
        }

        while (canStartMore()) {
            int nextIndex;
            BigInteger nextTransferHash;
            synchronized (this) {
                nextIndex = waitingChunks.firstKey();
                nextTransferHash = waitingChunks.remove(waitingChunks.firstKey());
                concurrentTransfers++;
            }

            try {
                localPeer.getTransferManager().download(
                        FileDownloadContinuation.transferDir
                                + fileName + "." + nextIndex, nextTransferHash, true, this, true);
            } catch (IOException e) {
                synchronized (this) { concurrentTransfers--; }
                System.err.println("File download failed");
            }
        }
    }

    @Override
    public DirectTransfer notifyFailed(DirectTransfer transfer) {
        System.out.println("File chunk download failed");
        DirectTransfer newTransfer = super.notifyFailed(transfer);
        if (newTransfer == null)
            synchronized (this) { concurrentTransfers--; }
        return newTransfer;
    }
}
