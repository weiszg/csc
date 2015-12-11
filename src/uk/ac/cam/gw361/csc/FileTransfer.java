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
public class FileTransfer extends Thread {
    // todo: error handling

    Socket socket = null;
    ServerSocket ssocket = null;
    FileOutputStream fileOutputStream = null;
    FileInputStream fileInputStream = null;
    DhtPeerAddress owner = null;
    BigInteger fileHash;
    LocalPeer localPeer;
    DhtPeerAddress remotePeer;
    byte[] data = new byte[8192];

    public FileTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                        FileOutputStream fileOutputStream, BigInteger fileHash,
                        DhtPeerAddress owner) {
        // download file, server mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileOutputStream = fileOutputStream;
        this.owner = owner;
    }

    public FileTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                        FileOutputStream fileOutputStream, BigInteger fileHash,
                        DhtPeerAddress owner) {
        // download file, client mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileOutputStream = fileOutputStream;
        this.owner = owner;
    }

    public FileTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, ServerSocket socket,
                        FileInputStream fileInputStream, BigInteger fileHash) {
        // upload mode, server mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileInputStream = fileInputStream;
    }

    public FileTransfer(LocalPeer localPeer, DhtPeerAddress remotePeer, Socket socket,
                        FileInputStream fileInputStream, BigInteger fileHash) {
        // upload file, client mode
        this.remotePeer = remotePeer;
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileInputStream = fileInputStream;
    }

    private void download() throws IOException {
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        try {
            InputStream inputStream = socket.getInputStream();
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            System.out.println("Starting download");

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

                // complete download, add to list of local files
                localPeer.getFileStore().addFile(new DhtFile(fileHash, totalRead, owner));
                // maybe we are the next owners
                localPeer.getFileStore().refreshResponsibility(fileHash,
                        localPeer.localAddress, false);
                System.out.println("Download complete");

                if (owner.equals(localPeer.localAddress)) {
                    // replicate to predecessors
                    localPeer.replicate(fileHash);
                }
            } else {
                System.err.println("Hash mismatch, expected: " + fileHash.toString() +
                        " got: " + realHash.toString());
                fileOutputStream.close();
                localPeer.getFileStore().removeFile(fileHash);
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
            System.out.println("Starting upload");
            int bytesRead;
            while ((bytesRead = bufferedInputStream.read(data, 0, data.length)) != -1) {
                outputStream.write(data, 0, bytesRead);
            }
            outputStream.flush();

            // upload complete, refresh responsibility for the file
            localPeer.getFileStore().refreshResponsibility(fileHash, remotePeer, false);
            System.out.println("Upload complete");
        } finally {
            outputStream.flush();
            bufferedInputStream.close();
            fileInputStream.close();
        }
    }

    public void run() {
        try {
            if (socket == null) {
                socket = ssocket.accept();
                ssocket.close();
            }

            if (fileInputStream != null)
                upload();
            else if (fileOutputStream != null)
                download();
            localPeer.notifyTransferCompleted(this, true);
        } catch (IOException ioe) {
            ioe.printStackTrace();
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
