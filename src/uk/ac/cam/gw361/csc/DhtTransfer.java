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
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        try {
            InputStream inputStream = socket.getInputStream();
            MessageDigest digest = MessageDigest.getInstance("SHA-1");

            int bytesRead;
            long totalRead = 0;
            while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
                bufferedOutputStream.write(data, 0, bytesRead);
                digest.update(data, 0, bytesRead);
                totalRead += bytesRead;
            }
            BigInteger realHash = new BigInteger(digest.digest());

            if (realHash.equals(fileHash)) {
                bufferedOutputStream.flush();
                bufferedOutputStream.close();
                fileOutputStream.close();

                if (continuation != null)
                    continuation.notifyFinished(this, totalRead);

                System.out.println("Download complete: " + socket.getPort()
                        + " - " + socket.getPort());
            } else {
                System.err.println("Hash mismatch, expected: " + fileHash.toString() +
                        " got: " + realHash.toString());
                fileOutputStream.close();
                localPeer.getDhtStore().removeFile(fileHash);
                throw new IOException();
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("No such algorithm");
        } finally {
            bufferedOutputStream.flush();
            bufferedOutputStream.close();
            fileOutputStream.close();
        }
    }

    private void upload() throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        OutputStream outputStream = socket.getOutputStream();
        try {
            System.out.println("Starting upload: " + socket.getLocalPort()
                    + " - " + socket.getPort());
            int bytesRead;
            while ((bytesRead = bufferedInputStream.read(data, 0, data.length)) != -1) {
                outputStream.write(data, 0, bytesRead);
            }
            outputStream.flush();

            // upload complete, refresh responsibility for the file
            localPeer.getDhtStore().refreshResponsibility(fileHash, remotePeer, false);
        } finally {
            outputStream.flush();
            bufferedInputStream.close();
            fileInputStream.close();
        }
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
            localPeer.notifyTransferCompleted(this, true);
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
            localPeer.notifyTransferCompleted(this, false);
        }
        finally {
            try {
                if (socket != null) socket.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}

interface TransferContinuation {
    void notifyFinished(DhtTransfer finishedTransfer, Long size) throws IOException;
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
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    out.write(buffer, 0, bytesRead);
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
            System.out.println("Uploaded " + finishedBlocks + "MB of " + meta.blocks + "MB");
        }

        while (concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty()) {
            int nextIndex = waitingChunks.firstKey();
            BigInteger nextTransfer = waitingChunks.remove(waitingChunks.firstKey());

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
    FileMetadata meta;
    TreeMap<Integer, BigInteger> waitingChunks;
    Set<DhtTransfer> runningTransfers = new HashSet<>();

    @Override
    public synchronized  void notifyFinished(DhtTransfer finishedTransfer, Long size)
            throws IOException {
        LocalPeer localPeer = finishedTransfer.localPeer;
        runningTransfers.remove(finishedTransfer);
        if (first) {
            // metadata download has finished, process metadata and start file blocks
            first = false;
            FileInputStream fis = new FileInputStream(transferDir +
                    finishedTransfer.fileHash.toString());
            ObjectInputStream ois = new ObjectInputStream(fis);
            try {
                meta = (FileMetadata) ois.readObject();
                waitingChunks = meta.getChunks();
            } catch (ClassNotFoundException e) {
                e.printStackTrace(); // invalid file or differing versions
                throw new IOException("Metadata download raised ClassNotFoundException");
            }
            System.out.println("Metadata collected");
        } else {
            finishedBlocks++;
            System.out.println("Downloaded " + finishedBlocks + "MB of " + meta.blocks + "MB");
        }

        while (concurrentTransfers < maxConcurrentTransfers && !waitingChunks.isEmpty()) {
            BigInteger nextTransfer = waitingChunks.remove(waitingChunks.firstKey());

            concurrentTransfers++;
            runningTransfers.add(localPeer.getClient().download(FileDownloadContinuation.transferDir
                    + finishedTransfer.fileHash.toString(), nextTransfer, this));
        }
    }
}