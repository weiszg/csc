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
    TransferContinuation continuation = null;
    LocalPeer localPeer;
    DhtPeerAddress remotePeer;
    boolean stopped = false;
    DhtFile transferFile;
    boolean isDownload;
    static boolean debug = false;
    public static long ratelimit = 0;
    static long lastTimestamp = 0;
    static long bytesSent = 0;

    protected TransferTask originalTask;
    void setOriginalTask(TransferTask originalTask) { this.originalTask = originalTask; }

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                          String targetName, boolean isDownload, DhtFile transferFile,
                          TransferContinuation continuation) {
        // server mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.transferFile = transferFile;
        this.targetName = targetName;
        this.isDownload = isDownload;
        this.continuation = continuation;
    }

    public DirectTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                          String targetName, boolean isDownload, DhtFile transferFile,
                          TransferContinuation continuation) {
        // client mode
        this.remotePeer = remotePeer;
        this.localPeer = localPeer;
        this.socket = socket;
        this.transferFile = transferFile;
        this.targetName = targetName;
        this.isDownload = isDownload;
        this.continuation = continuation;
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


        try (
                FileOutputStream os = new FileOutputStream(targetName + ".part");
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(os)
        ) {
            InputStream inputStream = socket.getInputStream();

            int bytesRead;
            while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
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
            if (!FileList.checkTimestamp(targetName + ".part",
                    ((SignedFile) transferFile).timestamp)) {
                throw new IOException("Timestamp mismatch!");
            }
        }

        if (debug) System.out.println("Download complete: " + socket.getLocalPort()
                + " - " + socket.getPort());
    }

    private static void sleepMillis(long millis) {
        if (millis < 0) return;
        try { Thread.sleep(millis); } catch (InterruptedException e) { }
    }

    private synchronized static void limitedWrite(OutputStream outputStream, byte[] data, int length)
            throws IOException {
        if (ratelimit > 0) {
            long time = System.currentTimeMillis();
            if (bytesSent + length > ratelimit) {
                sleepMillis(lastTimestamp + 1000 - time);
                time = System.currentTimeMillis();
                lastTimestamp = time;
                bytesSent = 0;
            }
            bytesSent += length;
        }
        outputStream.write(data, 0, length);
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
                    limitedWrite(outputStream, data, bytesWritten);
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
                    try {
                        Files.move(new File(targetName + ".part").toPath(),
                                new File(targetName).toPath(),
                                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        System.err.println("Unable to rename " + targetName + ": " + e.toString());
                        success = false;
                    }
                } else {
                    // clean up
                    new File(targetName + ".part").delete();
                }

            if (callContinuation && continuation != null)
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
            catch (IOException ioe) { if (!stopped) ioe.printStackTrace(System.out); }
        }
    }
}
