package uk.ac.cam.gw361.csc.storage;

import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalPeer;

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

    public DhtStore(LocalPeer localPeer, boolean scan) {
        this.localPeer = localPeer;
        myFolder = new File(storeDir + localPeer.userName);
        if (!scan) deleteFolder(myFolder);

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

        if (scan) {
            // read contents of the folder
            File[] listOfFiles = myFolder.listFiles();
            for (File f : listOfFiles) {
                if (f.isFile()) {
                    try {
                        if (f.getName().endsWith(".signed")) {
                            BigInteger key = new BigInteger(f.getName().substring(0,
                                    f.getName().length() - ".signed".length()));
                            addFile(new SignedFile(key, f.length(), localPeer.localAddress,
                                    FileList.loadTimestamp(f)));
                            if (debug) System.out.println("Signed file found " + f.getName());
                        } else {
                            BigInteger key = new BigInteger(f.getName());
                            // the owner doesn't matter, replicas could/should be deleted
                            addFile(new DhtFile(key, f.length(), localPeer.localAddress));
                            if (debug) System.out.println("File found " + f.getName());
                        }
                    } catch (NumberFormatException | IOException e) {
                    }
                }
            }
        }
    }

    public static boolean deleteFolder(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (int i=0; i<files.length; i++) {
                    if (files[i].isDirectory()) {
                        deleteFolder(files[i]);
                    }
                    else {
                        files[i].delete();
                    }
                }
            }
        }
        return (directory.delete());
    }

    public synchronized DhtFile getFile(BigInteger file) throws IOException {
        if (!files.containsKey(file))
            throw new IOException("File not found: " + file.toString());
        else return files.get(file);
    }

    public String getFolder() { return storeDir + localPeer.userName; }

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

    public synchronized boolean containsFile(BigInteger file) {
        return files.containsKey(file);
    }

    public synchronized boolean hasFile(DhtFile file) {
        DhtFile storedFile = files.get(file.hash);
        if (storedFile != null)
            // if either of them isn't a SignedFile then we're done, we have the same file
            // otherwise we also have to check if we store a later or equal version
            // note it is important that an existing content-signed file can't be overwritten
            // by a SignedFile
            return (!(storedFile instanceof SignedFile) || !(file instanceof SignedFile) ||
                    ((SignedFile) file).timestamp == null ||
                    ((SignedFile) storedFile).timestamp >= ((SignedFile) file).timestamp);
        else return false;
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

    public synchronized HashSet<DhtFile> getStoredFiles() {
        return new HashSet<>(files.values());
    }

    public synchronized void print(PrintStream out, String beginning) {
        for (BigInteger file : files.keySet()) {
            out.print(beginning);
            DhtFile dhtFile = files.get(file);
            if (dhtFile.owner.equals(localPeer.localAddress))
                out.print("xxx ");
            else
                out.print("    ");
            out.print(file.toString() + "  ");
            if (dhtFile instanceof SignedFile)
                out.print("SIGNED  ");
            out.println(dhtFile.lastQueried.toString());
        }
    }
}
