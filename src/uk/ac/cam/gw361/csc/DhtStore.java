package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by gellert on 06/11/2015.
 */
public class DhtStore {
    static final String storeDir = "./storage/";
    private static final boolean debug = true;
    private LocalPeer localPeer;
    // files and their properties stored locally
    private HashMap<BigInteger, DhtFile> files = new HashMap<>();
    // per-owner replicas we store locally
    private HashMap<DhtPeerAddress, HashSet<BigInteger>> responsibilities = new HashMap<>();
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

    public synchronized DhtFile getFile(BigInteger file) throws IOException {
        if (!files.containsKey(file))
            throw new IOException("File not found");
        else return files.get(file);
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
                System.err.println("DhtStore broken, file not found: " + myFolder.getPath()
                        + "/" + file.toString());
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
            System.err.println("DhtStore broken, file not found: " + myFolder.getPath()
                    + "/" + file.toString());
            return null;
        }
    }

    public synchronized void removeFile(BigInteger file) {
        System.out.println("Removing file " + file.toString());
        if (!files.containsKey(file))
            System.out.println("File didn't even exist " + file.toString());
        else {
            DhtFile dhtFile = files.get(file);
            removeResponsibility(dhtFile);
            files.remove(file);
        }
        File toRemove = new File(myFolder.getPath() + "/" + file.toString());
        toRemove.delete();
    }

    public synchronized void addFile(DhtFile file) {
        files.put(file.hash, file);
        addResponsibility(file);
    }

    private void addResponsibility(DhtFile file) {
        if (responsibilities.containsKey(file.owner))
            responsibilities.get(file.owner).add(file.hash);
        else {
            HashSet<BigInteger> ll = new HashSet<>();
            ll.add(file.hash);
            responsibilities.put(file.owner, ll);
        }
    }

    private void removeResponsibility(DhtFile file) {
        if (responsibilities.containsKey(file.owner)) {
            Set<BigInteger> respFiles = responsibilities.get(file.owner);
            respFiles.remove(file.hash);
            if (respFiles.isEmpty())
                responsibilities.remove(file.owner);
        }
    }

    public synchronized Long getLength(BigInteger file) throws IOException {
        if (!files.containsKey(file))
            throw new IOException("File not found");
        else
            return files.get(file).size;
    }

    public synchronized BigInteger getRealHash(BigInteger file) throws IOException {
        if (!files.containsKey(file))
            throw new IOException("File not found");
        else
            return files.get(file).realHash;
    }

    public synchronized boolean containsFile(BigInteger file) {
        return files.containsKey(file);
    }

    public synchronized boolean hasFile(DhtFile file) {
        return (files.containsKey(file.hash)
                && files.get(file.hash).realHash.equals(file.realHash));
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
                // remove old responsibility association
                removeResponsibility(oldFile);
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
        List<BigInteger> fileHashes;
        if (responsibilities.containsKey(peer))
            fileHashes = new LinkedList<>(responsibilities.get(peer));
        else
            fileHashes = new LinkedList<>();
        List<DhtFile> respFiles = new LinkedList<>();

        for (BigInteger hash : fileHashes)
            if (files.containsKey(hash))
                respFiles.add(files.get(hash));

        return respFiles;
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
    BigInteger hash;
    BigInteger realHash; // realHash=hash except for signed content
    Long size;
    DhtPeerAddress owner;
    transient Date lastQueried;

    DhtFile(BigInteger hash, Long size, DhtPeerAddress owner) {
        this.hash = hash;
        this.realHash = hash;
        this.size = size;
        this.owner = owner;
        this.lastQueried = new Date();
    }

    DhtFile(BigInteger hash, BigInteger realHash, Long size, DhtPeerAddress owner) {
        this.hash = hash;
        if (realHash != null)
            this.realHash = realHash;
        else
            this.realHash = hash;
        this.size = size;
        this.owner = owner;
        this.lastQueried = new Date();
    }
}