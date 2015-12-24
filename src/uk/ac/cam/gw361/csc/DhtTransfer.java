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

