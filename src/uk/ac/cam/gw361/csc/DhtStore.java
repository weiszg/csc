package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by gellert on 06/11/2015.
 */
public class DhtStore {
    static final String storeDir = "./storage";
    private static final boolean debug = true;
    private LocalPeer localPeer;
    // files and their properties stored locally
    private HashMap<BigInteger, DhtFile> files = new HashMap<>();
    // per-owner replicas we store locally
    private HashMap<DhtPeerAddress, HashSet<DhtFile>> responsibilities = new HashMap<>();
    // private folder used for storage
    private File myFolder;

    public DhtStore(LocalPeer localPeer) {
        this.localPeer = localPeer;
        myFolder = new File(storeDir + localPeer.userName);

        if (!myFolder.exists()) {
            File storageFolder = new File(storeDir);
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
                    // the owner doesn't matter, replicas could/should be deleted
                    addFile(new DhtFile(key, f.length(), localPeer.localAddress));
                    if (debug) System.out.println("File found " + f.getName());
                } catch (NumberFormatException e) { }
            }
        }
    }

    public synchronized FileInputStream readFile(BigInteger file) throws IOException {
        if (!files.containsKey(file))
            throw new IOException("File not found");
        else {
            try {
                FileInputStream fis = new FileInputStream(myFolder.getPath()
                        + "/" + file.toString());
                return fis;
            } catch (FileNotFoundException fnf) {
                System.err.println("DhtStore broken, file not found");
                return null;
            }
        }
    }

    public synchronized FileOutputStream writeFile(BigInteger file) {
        if (files.containsKey(file))
            System.out.println("Overwriting file " + file.toString());
        try {
            FileOutputStream fos = new FileOutputStream(myFolder.getPath()
                    + "/" + file.toString());
            return fos;
        } catch (FileNotFoundException fnf) {
            System.err.println("DhtStore broken, file not found");
            return null;
        }
    }

    public synchronized void removeFile(BigInteger file) {
        System.out.println("Removing file " + file.toString());
        files.remove(file);
        File toRemove = new File(myFolder.getPath() + "/" + file.toString());
        toRemove.delete();
    }

    public synchronized void addFile(DhtFile file) {
        files.put(file.fileHash, file);
        addResponsibility(file);
    }

    private void addResponsibility(DhtFile file) {
        if (responsibilities.containsKey(file.owner))
            responsibilities.get(file.owner).add(file);
        else {
            HashSet<DhtFile> ll = new HashSet<>();
            ll.add(file);
            responsibilities.put(file.owner, ll);
        }
    }

    public synchronized Long getLength(BigInteger file) throws IOException {
        if (!files.containsKey(file))
            throw new IOException("File not found");
        else
            return files.get(file).size;
    }

    public synchronized boolean containsFile(BigInteger file) {
        return files.containsKey(file);
    }

    public synchronized boolean refreshResponsibility(BigInteger file, DhtPeerAddress owner,
                                                   boolean force) {
        // update responsibility for the file's owner to new owner but only if owner is closer
        // to the file than the previous owner. Returns whether change has been made.
        // set responsibility anyway if force is true
        if (files.containsKey(file)) {
            DhtFile oldFile = files.get(file);
            // refresh last queried time
            oldFile.lastQueried = new Date();
            if (!oldFile.owner.equals(owner) && (force || owner.isBetween(oldFile.owner,
                    new DhtPeerAddress(file, null, null, localPeer.localAddress.getUserID())))) {

                if (responsibilities.containsKey(oldFile.owner)) {
                    Set<DhtFile> respFiles = responsibilities.get(oldFile.owner);
                    respFiles.remove(oldFile);
                    if (respFiles.isEmpty())
                        responsibilities.remove(oldFile.owner);
                }
                oldFile.owner = owner;
                addResponsibility(oldFile);
                return true;
            }
        }
        return false;
    }

    public synchronized void vacuum() {
        List<BigInteger> toRemove = new LinkedList<>();

        for (BigInteger fileHash : files.keySet()) {
            DhtFile file = files.get(fileHash);
            if (!file.owner.equals(localPeer.localAddress)) {
                // check timestamp
                Date now = new Date();
                long ageSeconds = (now.getTime() - file.lastQueried.getTime()) / 1000;
                if (ageSeconds > 30)
                    toRemove.add(fileHash);
            }
        }

        for (BigInteger fileHash : toRemove)
            removeFile(fileHash);
    }

    public synchronized List<DhtFile> getResponsibilitiesFor(DhtPeerAddress peer) {
        List<DhtFile> files;
        if (responsibilities.containsKey(peer))
            files = new LinkedList<>(responsibilities.get(peer));
        else
            files = new LinkedList<>();
        return files;
    }

    public synchronized void print(PrintStream out, String beginning) {
        for (BigInteger file : files.keySet()) {
            out.print(beginning);
            if (files.get(file).owner.equals(localPeer.localAddress))
                out.print("xxx ");
            else
                out.print("    ");
            out.print(file.toString() + "  ");
            out.println(files.get(file).lastQueried.toString());
        }
    }
}

class DhtFile implements Serializable {
    BigInteger fileHash;
    Long size;
    DhtPeerAddress owner;
    transient Date lastQueried;

    DhtFile(BigInteger fileHash, Long size, DhtPeerAddress owner) {
        this.fileHash = fileHash;
        this.size = size;
        this.owner = owner;
        this.lastQueried = new Date();
    }
}