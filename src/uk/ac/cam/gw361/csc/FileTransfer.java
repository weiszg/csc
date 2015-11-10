package uk.ac.cam.gw361.csc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by gellert on 10/11/2015.
 */
public class FileTransfer extends Thread {
    // todo: error handling

    Socket socket = null;
    ServerSocket ssocket = null;
    FileOutputStream fileOutputStream = null;
    FileInputStream fileInputStream = null;
    byte[] data = new byte[100000];

    public FileTransfer(ServerSocket socket, FileOutputStream fileOutputStream) {
        this.ssocket = socket;
        this.fileOutputStream = fileOutputStream;
    }

    public FileTransfer(Socket socket, FileOutputStream fileOutputStream) {
        this.socket = socket;
        this.fileOutputStream = fileOutputStream;
    }

    public FileTransfer(ServerSocket socket, FileInputStream fileInputStream) {
        this.ssocket = socket;
        this.fileInputStream = fileInputStream;
    }

    public FileTransfer(Socket socket, FileInputStream fileInputStream) {
        this.socket = socket;
        this.fileInputStream = fileInputStream;
    }

    private void download() throws IOException {
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        try {
            InputStream inputStream = socket.getInputStream();
            System.out.println("Starting download");

            int bytesRead = 0;
            while (bytesRead != -1) {
                bytesRead = inputStream.read(data, 0, data.length);
                bufferedOutputStream.write(data, 0, bytesRead);
            }
            System.out.println("Download complete");
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
            int bytesRead = 0;
            while (bytesRead != -1) {
                bytesRead = bufferedInputStream.read(data, 0, data.length);
                outputStream.write(data, 0, bytesRead);
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
                download();
            else if (fileOutputStream != null)
                upload();
        } catch (IOException ioe) {
            ioe.printStackTrace();
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
