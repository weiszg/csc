package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.security.PublicKey;
import java.security.SignedObject;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by gellert on 24/12/2015.
 */
public abstract class TransferContinuation {
    abstract void notifyFinished(DhtTransfer finishedTransfer) throws IOException;

    void notifyFinished(DhtTransfer finishedTransfer, Long transferSize)
            throws IOException {
        notifyFinished(finishedTransfer);
    }
}

class InternalUploadContinuation extends  TransferContinuation {
    @Override
    public void notifyFinished(DhtTransfer finishedTransfer) throws IOException {
        // upload complete, refresh responsibility for the file
        finishedTransfer.localPeer.getDhtStore().refreshResponsibility(
                finishedTransfer.fileHash, finishedTransfer.remotePeer, false);
    }
}

class InternalDownloadContinuation extends TransferContinuation {
    private DhtPeerAddress owner;

    InternalDownloadContinuation(DhtPeerAddress owner) {
        this.owner = owner;
    }

    @Override
    public void notifyFinished(DhtTransfer finishedTransfer) throws IOException {
        throw new IOException("No transfer length specified for InternalDownloadContinuation");
    }

    @Override
    public void notifyFinished(DhtTransfer finishedTransfer, Long transferSize) throws IOException {
        // complete download, add to list of local files
        LocalPeer localPeer = finishedTransfer.localPeer;
        BigInteger fileHash = finishedTransfer.fileHash;
        if (owner != null) {
            DhtFile downloadedFile = new DhtFile(fileHash, transferSize, owner);
            localPeer.getDhtStore().addFile(downloadedFile);
            // maybe we are the next owners
            localPeer.getDhtStore().refreshResponsibility(fileHash,
                    localPeer.localAddress, false);

            if (owner.equals(localPeer.localAddress)) {
                // replicate to predecessors
                finishedTransfer.localPeer.replicate(downloadedFile);
            }
        }
    }
}

class FileUploadContinuation extends TransferContinuation {
    static String transferDir = "./uploads/";
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
    Set<DhtTransfer> runningTransfers = new HashSet<>();

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

        lastName = file;
        if (file.contains("/"))
            lastName = file.substring(file.lastIndexOf("/"));
        fileName = FileUploadContinuation.transferDir + lastName;

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
                File newFile = new File(fileName + "." + index++);
                try (BufferedOutputStream bos = new BufferedOutputStream(
                        new FileOutputStream(newFile))) {
                    bos.write(buffer, 0, bytesRead);
                }
            }
        }
    }

    @Override
    public synchronized  void notifyFinished(DhtTransfer finishedTransfer) throws IOException {
        LocalPeer localPeer = finishedTransfer.localPeer;
        runningTransfers.remove(finishedTransfer);
        if (first) {
            // metadata upload has finished, process metadata and start file blocks
            first = false;
            // save the hash of the metadata, the entry point for the file
            metaHash = finishedTransfer.fileHash;
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
                    localPeer.saveFileList();
                    runningTransfers.add(localPeer.getClient().signedUpload(
                            localPeer.fileListPath, localPeer.localAddress.getUserID(), this));
                    fileListUpdated = true;
                } else {
                    System.out.println("Done.");
                }
            }
        }

        while (concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty()) {
            int nextIndex = waitingChunks.firstKey();
            waitingChunks.remove(waitingChunks.firstKey());

            concurrentTransfers++;
            runningTransfers.add(localPeer.getClient().upload(fileName + "." + nextIndex, this));
        }
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
    Set<DhtTransfer> runningTransfers = new HashSet<>();

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
    public synchronized  void notifyFinished(DhtTransfer finishedTransfer) throws IOException {
        LocalPeer localPeer = finishedTransfer.localPeer;
        runningTransfers.remove(finishedTransfer);
        if (first) {
            // metadata download has finished, process metadata and start file blocks
            first = false;
            FileInputStream fis = new FileInputStream(transferDir + fileName + ".meta");
            ObjectInputStream ois = new ObjectInputStream(fis);
            try {
                meta = (FileMetadata) ois.readObject();
                waitingChunks = meta.getChunks();
            } catch (ClassNotFoundException e) {
                e.printStackTrace(); // invalid file or differing versions
                throw new IOException("Metadata download raised ClassNotFoundException");
            }
            System.out.println("Metadata collected");

            // delete metadata
            new File(transferDir + fileName + ".meta").delete();
        } else {
            finishedBlocks++;
            concurrentTransfers--;
            System.out.println("Downloaded " + finishedBlocks + "MB of " + meta.blocks + "MB");

            // if done merge downloaded chunks
            if (waitingChunks.isEmpty() && concurrentTransfers==0) {
                mergeFile();
                System.out.println("Merging succesful, download complete");
            }
        }

        while (concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty()) {
            int nextIndex = waitingChunks.firstKey();
            BigInteger nextTransfer = waitingChunks.remove(waitingChunks.firstKey());

            concurrentTransfers++;
            runningTransfers.add(localPeer.getClient().download(FileDownloadContinuation.transferDir
                    + fileName + "." + nextIndex, nextTransfer, true, this));
        }
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
    public synchronized  void notifyFinished(DhtTransfer finishedTransfer) throws IOException {
        LocalPeer localPeer = finishedTransfer.localPeer;

        FileList fileList = FileList.load(fileName, publicKey);
        if (fileList == null) {
            System.err.println("Error reading file list");
        } else {
            localPeer.setLastQueriedFileList(fileList);
        }
    }
}
