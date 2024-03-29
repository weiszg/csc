package uk.ac.cam.gw361.csc.storage;

import uk.ac.cam.gw361.csc.dht.SignedFileList;

import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by gellert on 25/12/2015.
 */
public class FileList implements Serializable {
    private Map<String, BigInteger> files = new HashMap<>();
    private long lastModified = System.currentTimeMillis() / 1000;

    private static final boolean debug = false;

    public synchronized long getLastModified() {
        return lastModified;
    }

    public synchronized void put(String fileName, BigInteger hash) {
        files.put(fileName, hash);
        lastModified = System.currentTimeMillis() / 1000;
    }

    public synchronized List<String> getFileList() {
        List<String> result = new LinkedList<>();
        for (String file : files.keySet())
            result.add(file);
        return result;
    }

    public synchronized BigInteger get(String file) {
        return files.get(file);
    }

    public static boolean checkTimestamp(File file, Long timestamp) {
        // check whether the public timestamp of a signed file equals to the provided timestamp
        // for downloads we accept any timestamp by setting timestamp to null
        if (timestamp == null) return true;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            Object myobj = ois.readObject();
            if (myobj instanceof SignedFileList)
                return ((SignedFileList) myobj).getLastModified()==timestamp;
            else return false;
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failing to check the timestamp of file " + file + ": " + e.toString());
            return false;
        }
    }

    static long loadTimestamp(File file) throws IOException {
        // check whether the public timestamp of a signed file equals to the provided timestamp
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
        Object myobj;
        try { myobj = ois.readObject(); }
        catch (ClassNotFoundException e) { throw new IOException("ClassNotFound on loadTimestamp"); }
        if (myobj instanceof SignedFileList)
            return ((SignedFileList) myobj).getLastModified();
        else throw new IOException("File format isn't SignedFileList");
    }

    public static FileList load(String file, PublicKey publicKey) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            Object myobj = ois.readObject();
            if (myobj instanceof SignedFileList)
                return getVerified((SignedFileList) myobj, publicKey);
            else return null;
        } catch (ClassNotFoundException | SignatureMismatchException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    public static FileList loadOrCreate(String file, PublicKey publicKey) {
        FileList fileList = load(file, publicKey);
        if (fileList == null) {
            System.out.println("Creating empty file list");
            return new FileList();
        } else return fileList;
    }

    public static KeyPair initKeys(String keyPath) {
        // load previously saved keypair or create a new keypair
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(keyPath
                    + "private.key"));
            Object privObj = ois.readObject();
            ois.close();
            ois = new ObjectInputStream(new FileInputStream(keyPath
                    + "public.key"));
            Object pubObj = ois.readObject();
            ois.close();

            if (pubObj instanceof PublicKey && privObj instanceof PrivateKey) {
                return new KeyPair((PublicKey) pubObj, (PrivateKey) privObj);
            } else return createKeys(keyPath);
        } catch (IOException | ClassNotFoundException e) {
            return createKeys(keyPath);
        }
    }

    private static KeyPair createKeys(String keyPath) {
        PrivateKey privateKey = null;
        PublicKey publicKey = null;
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("DSA");
            SecureRandom random;
            try {
                random = SecureRandom.getInstance("NativePRNGBlocking");
                System.out.println("Creating new keys with a blocking source of randomness...");
            } catch (NoSuchAlgorithmException e) {
                // falling back to SHA1PRNG
                random = SecureRandom.getInstance("SHA1PRNG");
            }
            keyGen.initialize(1024, random);
            KeyPair keyPair = keyGen.generateKeyPair();
            privateKey = keyPair.getPrivate();
            publicKey = keyPair.getPublic();
            System.out.println("Created new keys");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ObjectOutputStream ous = new ObjectOutputStream(new FileOutputStream(keyPath +
                    "private.key"));
            ous.writeObject(privateKey);
            ous.flush();
            ous.close();

            ous = new ObjectOutputStream(new FileOutputStream(keyPath +
                    "public.key"));
            ous.writeObject(publicKey);
            ous.flush();
            ous.close();
            return new KeyPair(publicKey, privateKey);
        } catch (IOException e) {
            return null;
        }
    }

    public SignedFileList getSignedVersion(PrivateKey privateKey) throws IOException {
        Signature dsa;
        try {
            dsa = Signature.getInstance("SHA256withDSA");
            SignedObject so = new SignedObject(this, privateKey, dsa);
            SignedFileList sf = new SignedFileList(so, lastModified);
            return sf;
        } catch (NoSuchAlgorithmException | InvalidKeyException |
                SignatureException | IOException e) {
            throw new IOException(e.toString());
        }
    }

    private static FileList getVerified(SignedFileList sf, PublicKey publicKey)
            throws SignatureMismatchException, IOException {
        Signature dsa;
        try {
            dsa = Signature.getInstance("SHA256withDSA");
            if (debug) System.out.println(publicKey.toString());

            SignedObject so = sf.getSignedObject();
            if (so.verify(publicKey, dsa)) {
                Object myobj = so.getObject();

                if (myobj instanceof FileList) {
                    FileList result = (FileList) myobj;

                    // check if extracted result's timestamp matches with the SignedFileList's
                    // publicly advertised timestamp, as otherwise the file is corrupt
                    if (result.lastModified != sf.getLastModified())
                        throw new SignatureMismatchException();

                    return result;
                }
                else throw new SignatureMismatchException();
            } else throw new SignatureMismatchException();
        } catch (NoSuchAlgorithmException | InvalidKeyException |
                SignatureException | IOException | ClassNotFoundException e) {
            throw new IOException(e.toString());
        }
    }
}


class SignatureMismatchException extends Exception {

}
