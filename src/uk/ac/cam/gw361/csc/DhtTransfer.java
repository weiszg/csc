package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by gellert on 10/11/2015.
 */
public class DhtTransfer extends Thread {
    Socket socket = null;
    ServerSocket ssocket = null;
    FileOutputStream fileOutputStream = null;
    FileInputStream fileInputStream = null;
    TransferContinuation continuation = null;
    BigInteger fileHash;
    LocalPeer localPeer;
    DhtPeerAddress remotePeer;
    boolean stopped = false;
    boolean hashCheck = true; // to disregard hash checks for signed content downloads

    protected TransferTask originalTask;
    void setOriginalTask(TransferTask originalTask) { this.originalTask = originalTask; }

    public DhtTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                       FileOutputStream fileOutputStream, BigInteger fileHash,
                       boolean hashCheck, TransferContinuation continuation) {
        // download file, server mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileOutputStream = fileOutputStream;
        this.hashCheck = hashCheck;
        this.continuation = continuation;
    }

    public DhtTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                       FileOutputStream fileOutputStream, BigInteger fileHash,
                       Boolean hashCheck, TransferContinuation continuation) {
        // download file, client mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileOutputStream = fileOutputStream;
        this.hashCheck = hashCheck;
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
        byte[] data = new byte[8192];
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

            if (hashCheck && !realHash.equals(fileHash)) {
                System.err.println("Hash mismatch, expected: " + fileHash.toString() +
                        " got: " + realHash.toString());
                fileOutputStream.close();
                localPeer.getDhtStore().removeFile(fileHash);
                throw new IOException();
            }
            bufferedOutputStream.flush();
        }

        System.out.println("Download complete: " + socket.getLocalPort()
                + " - " + socket.getPort());
        stopTransfer(true, totalRead);
    }

    private void upload() throws IOException {
        byte[] data = new byte[8192];
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

        stopTransfer(true);
    }

    synchronized void stopTransfer(boolean success) {
        stopTransfer(success, null);
    }

    synchronized void stopTransfer(boolean success, Long transferSize) {
        // called internally when the transfer has naturally finished
        // or externally to stop transfer either because the target we upload to has the
        // file already (success) or we are out of range (failure)
        if (!stopped) {
            stopped = true;
            if (continuation != null)
                if (success) {
                    if (transferSize == null)
                        continuation.notifyFinished(this);
                    else
                        continuation.notifyFinished(this, transferSize);
                } else {
                    continuation.notifyFailed(this);
                }
            localPeer.notifyTransferCompleted(this, success);
        }
    }

    public void run() {
        try {
            if (socket == null) {
                ssocket.setSoTimeout(2000);
                socket = ssocket.accept();
                ssocket.close();
            }

            if (fileInputStream != null)
                upload();
            else if (fileOutputStream != null)
                download();

        } catch (IOException ioe) {
            if (!stopped)
                System.out.println("Transfer failed due to: " + ioe.toString());
            stopTransfer(false);
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
