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
public class DirectTransfer extends Thread {
    Socket socket = null;
    ServerSocket ssocket = null;
    FileOutputStream fileOutputStream = null;
    FileInputStream fileInputStream = null;
    TransferContinuation continuation = null;
    LocalPeer localPeer;
    DhtPeerAddress remotePeer;
    boolean stopped = false;
    DhtFile transferFile;

    protected TransferTask originalTask;
    void setOriginalTask(TransferTask originalTask) { this.originalTask = originalTask; }
    // todo: set timestamp to what server thinks it is so that checking can be done later
    // checking module already implemented, need to do this to finish end-to-end checking
    // and then need to do peer-to-peer handling as well, essentially replacing realHash with
    // timestamp

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                          FileOutputStream fileOutputStream, DhtFile transferFile,
                          TransferContinuation continuation) {
        // download file, server mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileOutputStream = fileOutputStream;
        this.transferFile = transferFile;
        this.continuation = continuation;
    }

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                          FileOutputStream fileOutputStream, DhtFile transferFile,
                          TransferContinuation continuation) {
        // download file, client mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileOutputStream = fileOutputStream;
        this.transferFile = transferFile;
        this.continuation = continuation;
    }

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                          FileInputStream fileInputStream, DhtFile transferFile,
                          TransferContinuation continuation) {
        // upload mode, server mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileInputStream = fileInputStream;
        this.transferFile = transferFile;
        this.continuation = continuation;
    }

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                          FileInputStream fileInputStream, DhtFile transferFile,
                          TransferContinuation continuation) {
        // upload file, client mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileInputStream = fileInputStream;
        this.transferFile = transferFile;
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

            if (transferFile.checkHash(realHash)) {
                System.err.println("Hash mismatch, expected: " + transferFile.hash.toString() +
                        " got: " + realHash.toString());
                fileOutputStream.close();
                // make sure to delete the file from the file system
                localPeer.getDhtStore().removeFile(transferFile.hash);

                throw new IOException();
            }
            bufferedOutputStream.flush();
        }

        if (transferFile instanceof SignedFile) {
            // further checks are necessary to ensure that the public timestamp of the downloaded
            // data corresponds to the advertised timestamp
            if (!FileList.checkTimestamp(filename, ((SignedFile) transferFile).timestamp)) {
                System.err.println("Timestamp mismatch!");
                // make sure to delete the file from the file system
                localPeer.getDhtStore().removeFile(transferFile.hash);

                throw new IOException();
            }
        }

        System.out.println("Download complete: " + socket.getLocalPort()
                + " - " + socket.getPort());
        stopTransfer(true);
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
        // called internally when the transfer has naturally finished
        // or externally to stop transfer either because the target we upload to has the
        // file already (success) or we are out of range (failure)
        if (!stopped) {
            stopped = true;
            if (continuation != null)
                if (success) {
                    continuation.notifyFinished(this);
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
