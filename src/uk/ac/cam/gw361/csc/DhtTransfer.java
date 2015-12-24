package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by gellert on 10/11/2015.
 */
public class DhtTransfer extends Thread {
    // todo: error handling

    Socket socket = null;
    ServerSocket ssocket = null;
    FileOutputStream fileOutputStream = null;
    FileInputStream fileInputStream = null;
    TransferContinuation continuation = null;
    BigInteger fileHash;
    LocalPeer localPeer;
    DhtPeerAddress remotePeer;
    boolean stoppedWithSuccess = false;
    byte[] data = new byte[8192];

    public DhtTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                       FileOutputStream fileOutputStream, BigInteger fileHash,
                       TransferContinuation continuation) {
        // download file, server mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileOutputStream = fileOutputStream;
        this.continuation = continuation;
    }

    public DhtTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                       FileOutputStream fileOutputStream, BigInteger fileHash,
                       TransferContinuation continuation) {
        // download file, client mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileOutputStream = fileOutputStream;
        this.continuation = continuation;
    }

    public DhtTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                       FileInputStream fileInputStream, BigInteger fileHash,
                       TransferContinuation continuation) {
        // upload mode, server mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileInputStream = fileInputStream;
        this.continuation = continuation;
    }

    public DhtTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                       FileInputStream fileInputStream, BigInteger fileHash,
                       TransferContinuation continuation) {
        // upload file, client mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileInputStream = fileInputStream;
        this.continuation = continuation;
    }

    private void download() throws IOException {
        long totalRead = 0;

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("No such algorithm");
        }

        try (BufferedOutputStream bufferedOutputStream
                     = new BufferedOutputStream(fileOutputStream)) {
            InputStream inputStream = socket.getInputStream();

            int bytesRead;
            while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
                bufferedOutputStream.write(data, 0, bytesRead);
                digest.update(data, 0, bytesRead);
                totalRead += bytesRead;
            }
            BigInteger realHash = new BigInteger(digest.digest());

            if (!realHash.equals(fileHash)) {
                System.err.println("Hash mismatch, expected: " + fileHash.toString() +
                        " got: " + realHash.toString());
                fileOutputStream.close();
                localPeer.getDhtStore().removeFile(fileHash);
                throw new IOException();
            }
            bufferedOutputStream.flush();
        }

        System.out.println("Download complete: " + socket.getPort()
                + " - " + socket.getPort());

        if (continuation != null)
            continuation.notifyFinished(this, totalRead);
    }

    private void upload() throws IOException {
        long totalWritten = 0;
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
            try (OutputStream outputStream = socket.getOutputStream()) {
                System.out.println("Starting upload: " + socket.getLocalPort()
                        + " - " + socket.getPort());
                int bytesWritten;
                while ((bytesWritten = bufferedInputStream.read(data, 0, data.length)) != -1) {
                    outputStream.write(data, 0, bytesWritten);
                    totalWritten += bytesWritten;
                }
                outputStream.flush();
            }
        }

        if (continuation != null)
            continuation.notifyFinished(this, totalWritten);
    }

    synchronized void stopWithSuccess() {
        System.out.println("Stopping transfer because it's redundant");
        stoppedWithSuccess = true;

        if (continuation != null)
            try {
                continuation.notifyFinished(this, localPeer.getDhtStore().getLength(fileHash));
            } catch (IOException e) {
                e.printStackTrace();
            }

        localPeer.notifyTransferCompleted(this, true);
    }

    public void run() {
        try {
            if (socket == null) {
                ssocket.setSoTimeout(1000);
                socket = ssocket.accept();
                ssocket.close();
            }

            if (fileInputStream != null)
                upload();
            else if (fileOutputStream != null)
                download();

            synchronized (this) {
                if (!stoppedWithSuccess)
                    localPeer.notifyTransferCompleted(this, true);
            }
        } catch (IOException ioe) {
            synchronized (this) {
                if (!stoppedWithSuccess) {
                    System.out.println(ioe.toString());
                    localPeer.notifyTransferCompleted(this, false);
                }
            }
        }
        finally {
            try { if (fileInputStream != null) fileInputStream.close(); }
            catch (IOException ioe) { ioe.printStackTrace(); }

            try { if (fileOutputStream != null) fileOutputStream.close(); }
            catch (IOException ioe) { ioe.printStackTrace(); }

            try { if (socket != null) socket.close(); }
            catch (IOException ioe) { ioe.printStackTrace(); }
        }
    }
}

interface TransferContinuation {
    void notifyFinished(DhtTransfer finishedTransfer, Long size) throws IOException;
}

class InternalUploadContinuation implements  TransferContinuation {
    @Override
    public void notifyFinished(DhtTransfer finishedTransfer, Long size) throws IOException {
        // upload complete, refresh responsibility for the file
        finishedTransfer.localPeer.getDhtStore().refreshResponsibility(
                finishedTransfer.fileHash, finishedTransfer.remotePeer, false);
    }
}

class InternalDownloadContinuation implements TransferContinuation {
    private DhtPeerAddress owner;

    InternalDownloadContinuation(DhtPeerAddress owner) {
        this.owner = owner;
    }

    @Override
    public void notifyFinished(DhtTransfer finishedTransfer, Long size) throws IOException {
        // complete download, add to list of local files
        LocalPeer localPeer = finishedTransfer.localPeer;
        BigInteger fileHash = finishedTransfer.fileHash;
        if (owner != null) {
            localPeer.getDhtStore().addFile(new DhtFile(fileHash, size, owner));
            // maybe we are the next owners
            localPeer.getDhtStore().refreshResponsibility(fileHash,
                    localPeer.localAddress, false);

            if (owner.equals(localPeer.localAddress)) {
                // replicate to predecessors
                finishedTransfer.localPeer.replicate(fileHash);
            }
        }
    }
}

class FileUploadContinuation implements TransferContinuation {
    static String transferDir = "./uploads/";
    static int maxConcurrentTransfers = 5;
    int concurrentTransfers = 0;
    private boolean first = true;
    private int finishedBlocks = 0;
    private String fileName;
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

        String lastName = file;
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
    public synchronized  void notifyFinished(DhtTransfer finishedTransfer, Long size)
            throws IOException {
        LocalPeer localPeer = finishedTransfer.localPeer;
        runningTransfers.remove(finishedTransfer);
        if (first) {
            // metadata upload has finished, process metadata and start file blocks
            first = false;
            System.out.println("Metadata uploaded");
        } else {
            finishedBlocks++;
            concurrentTransfers--;
            System.out.println("Uploaded " + finishedBlocks + "MB of " + meta.blocks + "MB");
        }

        while (concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty()) {
            int nextIndex = waitingChunks.firstKey();
            waitingChunks.remove(waitingChunks.firstKey());

            concurrentTransfers++;
            runningTransfers.add(localPeer.getClient().upload(fileName + "." + nextIndex, this));
        }
    }
}

class FileDownloadContinuation implements TransferContinuation {
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
    public synchronized  void notifyFinished(DhtTransfer finishedTransfer, Long size)
            throws IOException {
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
                    + fileName + "." + nextIndex, nextTransfer, this));
        }
    }
}