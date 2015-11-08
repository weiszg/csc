package uk.ac.cam.gw361.csc;

import java.io.File;
import java.math.BigInteger;
import java.util.TreeMap;

/**
 * Created by gellert on 06/11/2015.
 */
public class FileStore {
    private LocalPeer localPeer;
    private TreeMap<BigInteger, Long> files = new TreeMap<>();

    public FileStore(LocalPeer localPeer) {
        this.localPeer = localPeer;
        File folder = new File("./" + localPeer.userName);
        File[] listOfFiles = folder.listFiles();
        /*
        for (File f : listOfFiles) {
            if (f.isFile()) {
                BigInteger key = new BigInteger(f.getName());
                System.out.println(key.toString());
                files.put(key, f.length());
                //what's this yo? System.out.println("File " + listOfFiles[i].getName());
            }
        }*/
    }
}
