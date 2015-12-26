package uk.ac.cam.gw361.csc;

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

    synchronized void put(String fileName, BigInteger hash) {
        files.put(fileName, hash);
    }

    synchronized List<String> getFileList() {
        List<String> result = new LinkedList<>();
        for (String file : files.keySet())
            result.add(file);
        return result;
    }

    synchronized BigInteger get(String file) {
        return files.get(file);
    }

    static FileList load(String file, PublicKey publicKey) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            Object myobj = ois.readObject();
            if (myobj instanceof SignedObject)
                return getVerified((SignedObject) myobj, publicKey);
            else return null;
        } catch (IOException | ClassNotFoundException | HashMismatchException e) {
            e.printStackTrace();
            return null;
        }
    }

    static FileList loadOrCreate(String file, PublicKey publicKey) {
        FileList fileList = load(file, publicKey);
        if (fileList == null) {
            System.out.println("Creating empty file list");
            return new FileList();
        } else return fileList;
    }

    static KeyPair initKeys(String keyPath) {
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
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("DSA", "SUN");
            SecureRandom random = SecureRandom.getInstanceStrong();
            keyGen.initialize(1024, random);
            KeyPair keyPair = keyGen.generateKeyPair();
            privateKey = keyPair.getPrivate();
            publicKey = keyPair.getPublic();
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
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
            e.printStackTrace();
            return null;
        }
    }

    SignedObject getSignedVersion(PrivateKey privateKey) throws IOException {
        Signature dsa;
        try {
            dsa = Signature.getInstance("SHA1withDSA", "SUN");
            SignedObject so = new SignedObject(this, privateKey, dsa);
            return so;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException |
                SignatureException | IOException e) {
            throw new IOException(e.toString());
        }
    }

    static FileList getVerified(SignedObject so, PublicKey publicKey)
            throws HashMismatchException, IOException {
        Signature dsa;
        try {
            dsa = Signature.getInstance("SHA1withDSA", "SUN");
            if (so.verify(publicKey, dsa)) {
                Object myobj = so.getObject();
                if (myobj instanceof FileList)
                    return (FileList) myobj;
                else throw new HashMismatchException();
            } else throw new HashMismatchException();
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException |
                SignatureException | IOException | ClassNotFoundException e) {
            throw new IOException(e.toString());
        }
    }
}

class HashMismatchException extends Exception {

}
