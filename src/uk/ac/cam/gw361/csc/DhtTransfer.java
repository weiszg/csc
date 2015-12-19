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

class FileDownloadContinuation implements TransferContinuation {
    @Override
    public void notifyFinished(DhtTransfer finishedTransfer, Long size) {

    }
}