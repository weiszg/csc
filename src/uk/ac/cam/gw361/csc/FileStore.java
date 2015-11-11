package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.util.TreeMap;

/**
 * Created by gellert on 06/11/2015.
 */
public class FileStore {
    private static final boolean debug = true;
    private LocalPeer localPeer;
    private TreeMap<BigInteger, Long> fileSize = new TreeMap<>();
    private File myFolder;

    public FileStore(LocalPeer localPeer) {
        this.localPeer = localPeer;
        myFolder = new File("./storage/" + localPeer.userName);

        if (!myFolder.exists()) {
            File storageFolder = new File("./storage");
            if (!storageFolder.exists())
                storageFolder.mkdir();
            System.out.println("creating directory: " + localPeer.userName);
            if (!myFolder.mkdir()) {
                System.err.println("creating folder failed");
                return;
            }
        }

        File[] listOfFiles = myFolder.listFiles();
        for (File f : listOfFiles) {
            if (f.isFile()) {
                try {
                    BigInteger key = new BigInteger(f.getName());
                    System.out.println(key.toString());
                    fileSize.put(key, f.length());
                    if (debug) System.out.println("File found " + f.getName());
                } catch (NumberFormatException e) { }
            }
        }
    }

    public synchronized FileInputStream readFile(BigInteger file) throws IOException {
        if (!fileSize.containsKey(file))
            throw new IOException("File not found");
        else {
            try {
                FileInputStream fis = new FileInputStream(myFolder.getPath()
                        + "/" + file.toString());
                return fis;
            } catch (FileNotFoundException fnf) {
                System.err.println("FileStore broken, file not found");
                return null;
            }
        }
    }

    public synchronized FileOutputStream writeFile(BigInteger file) {
        if (fileSize.containsKey(file))
            System.out.println("Overwriting file " + file.toString());
        try {
            FileOutputStream fos = new FileOutputStream(myFolder.getPath()
                    + "/" + file.toString());
            return fos;
        } catch (FileNotFoundException fnf) {
            System.err.println("FileStore broken, file not found");
            return null;
        }
    }

    public synchronized void setLength(BigInteger file, Long length) {
        fileSize.put(file, length);
    }

    public synchronized Long getLength(BigInteger file) throws IOException {
        if (!fileSize.containsKey(file))
            throw new IOException("File not found");
        else
            return fileSize.get(file);
    }
}
