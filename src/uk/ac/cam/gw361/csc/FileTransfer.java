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
    BigInteger fileHash;
    LocalPeer localPeer;
    byte[] data = new byte[8192];

    public FileTransfer(LocalPeer localPeer, ServerSocket socket,
                        FileOutputStream fileOutputStream, BigInteger fileHash) {
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileOutputStream = fileOutputStream;
    }

    public FileTransfer(LocalPeer localPeer, Socket socket,
                        FileOutputStream fileOutputStream, BigInteger fileHash) {
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.socket = socket;
        this.fileOutputStream = fileOutputStream;
    }

    public FileTransfer(LocalPeer localPeer, ServerSocket socket,
            FileInputStream fileInputStream, BigInteger fileHash) {
        this.fileHash = fileHash;
        this.localPeer = localPeer;
        this.ssocket = socket;
        this.fileInputStream = fileInputStream;
    }

    public FileTransfer(LocalPeer localPeer, Socket socket,
                        FileInputStream fileInputStream, BigInteger fileHash) {
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
                localPeer.getFileStore().setLength(fileHash, totalRead);
                System.out.println("Download complete");
            } else {
                System.err.println("Hash mismatch, expected: " + fileHash.toString() +
                        " got: " + realHash.toString());
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
            int totalRead = 0;
            while ((bytesRead = bufferedInputStream.read(data, 0, data.length)) != -1) {
                outputStream.write(data, 0, bytesRead);
                totalRead += bytesRead;
            }
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
