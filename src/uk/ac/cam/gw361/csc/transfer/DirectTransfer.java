package uk.ac.cam.gw361.csc.transfer;

import uk.ac.cam.gw361.csc.dht.PeerManager;
import uk.ac.cam.gw361.csc.storage.FileList;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.storage.SignedFile;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by gellert on 10/11/2015.
 */
public class DirectTransfer extends Thread {
    Socket socket = null;
    ServerSocket ssocket = null;
    String targetName;
    public TransferContinuation continuation = null;
    LocalPeer localPeer;
    DhtPeerAddress remotePeer;
    boolean stopped = false;
    DhtFile transferFile;
    boolean isDownload;
    static boolean debug = false;
    public final static Limiter externalLimiter = new Limiter(), internalLimiter = new Limiter();
    public final Limiter myLimiter;
    File tempFile;

    protected TransferTask originalTask;
    void setOriginalTask(TransferTask originalTask) { this.originalTask = originalTask; }

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                          String targetName, boolean isDownload, DhtFile transferFile,
                          TransferContinuation continuation, boolean externalDomain) {
        // server mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.transferFile = transferFile;
        this.targetName = targetName;
        this.isDownload = isDownload;
        this.continuation = continuation;
        myLimiter = externalDomain ? externalLimiter : internalLimiter;
    }

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                          String targetName, boolean isDownload, DhtFile transferFile,
                          TransferContinuation continuation, boolean externalDomain) {
        // client mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.socket = socket;
        this.transferFile = transferFile;
        this.targetName = targetName;
        this.isDownload = isDownload;
        this.continuation = continuation;
        myLimiter = externalDomain ? externalLimiter : internalLimiter;
    }

    private void download() throws IOException {
        byte[] data = new byte[8192];
        long totalRead = 0;

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("No such algorithm");
        }

        tempFile = File.createTempFile("csc-dl-part", ".tmp");
        tempFile.deleteOnExit();

        try (
                FileOutputStream os = new FileOutputStream(tempFile);
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(os)
        ) {
            InputStream inputStream = socket.getInputStream();

            int bytesRead;
            while ((bytesRead = myLimiter.limitedRead(inputStream, data, 0, data.length)) != -1) {
                bufferedOutputStream.write(data, 0, bytesRead);
                digest.update(data, 0, bytesRead);
                totalRead += bytesRead;
                PeerManager.reportBytesSent(true, bytesRead, true);
            }
            bufferedOutputStream.flush();
            os.flush();
            os.close();
        }

        BigInteger realHash = new BigInteger(digest.digest());
        if (!transferFile.checkHash(realHash)) {
            System.err.println("Hash mismatch, expected: " + transferFile.hash.toString() +
                    " got: " + realHash.toString());

            throw new IOException();
        }

        if (transferFile instanceof SignedFile) {
            // further checks are necessary to ensure that the public timestamp of the downloaded
            // data corresponds to the advertised timestamp
            if (!FileList.checkTimestamp(tempFile,
                    ((SignedFile) transferFile).timestamp)) {
                throw new IOException("Timestamp mismatch!");
            }
        }

        if (debug) System.out.println("Download complete: " + socket.getLocalPort()
                + " - " + socket.getPort());
    }

    private void upload() throws IOException {
        byte[] data = new byte[8192];
        long totalWritten = 0;

        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(
                new FileInputStream(targetName))) {
            try (OutputStream outputStream = socket.getOutputStream()) {
                if (debug) System.out.println("Starting upload: " + socket.getLocalPort()
                        + " - " + socket.getPort());
                int bytesWritten;
                while ((bytesWritten = bufferedInputStream.read(data, 0, data.length)) != -1) {
                    myLimiter.limitedWrite(outputStream, data, bytesWritten);
                    totalWritten += bytesWritten;
                    PeerManager.reportBytesSent(false, bytesWritten, true);
                }
                outputStream.flush();
            }
        }
    }

    synchronized public void stopTransfer(boolean success) {
        stopTransfer(success, true);
    }

    synchronized public void stopTransfer(boolean success, boolean callContinuation) {
        // called internally when the transfer has naturally finished
        // or externally to stop transfer either because the target we upload to has the
        // file already (success) or we are out of range (failure)
        if (!stopped) {
            stopped = true;

            if (isDownload)
                if (success) {
                    // solidify download
                    File targetFile = new File(targetName);
                    if (targetFile.exists()) targetFile.delete();
                    tempFile.renameTo(targetFile);
                } else {
                    // clean up
                    if (tempFile != null)
                        tempFile.delete();
                }

            if (callContinuation && continuation != null)
                if (success) {
                    continuation.notifyFinished(this);
                } else {
                    continuation.notifyFailed(this);
                }
        }
        localPeer.getTransferManager().notifyTransferCompleted(this, success);
    }

    public void run() {
        try {
            if (socket == null) {
                ssocket.setSoTimeout(2000);
                socket = ssocket.accept();
                ssocket.close();
            }

            if (transferFile instanceof SignedFile)
                targetName += ".signed";

            if (!isDownload)
                upload();
            else
                download();
            stopTransfer(true);

        } catch (IOException ioe) {
            if (!stopped) {
                System.out.println("Transfer " + transferFile.hash.toString() +
                        " failed due to: " + ioe.toString());
                stopTransfer(false);
            }
        }
        finally {
            try { if (socket != null) socket.close(); }
            catch (IOException ioe) {
                if (!stopped) {
                    ioe.printStackTrace(System.out);
                    localPeer.getTransferManager().notifyTransferCompleted(this, false);
                }
            }
        }
    }
}
