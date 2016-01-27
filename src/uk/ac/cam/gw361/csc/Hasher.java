package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by gellert on 10/11/2015.
 */
public class Hasher {
    public static BigInteger hashFile(String name) throws IOException {
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(name))) {
            return hashFile(is, 0);
        }
    }

    public static BigInteger hashFile(BufferedInputStream is, long size) throws IOException {
        // hashes size many bytes of the file read from the input stream
        // size=0 hashes the whole file
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");

            int n = 0;
            long totalRead = 0;
            byte[] buffer = new byte[8192];
            while (n != -1 && (size == 0 || totalRead < size)) {
                if (size == 0)
                    n = is.read(buffer);
                else
                    n = is.read(buffer, 0, (int)Math.min(8192, size-totalRead));
                totalRead += n;
                if (n > 0) {
                    digest.update(buffer, 0, n);
                }
            }
            return new BigInteger(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("No such algorithm");
        }
    }

    public static BigInteger hashString(String input) {
        MessageDigest cript = null;
        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(input.getBytes("utf8"));
            return new BigInteger(cript.digest());
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
            return BigInteger.ZERO;
        }
    }
}
