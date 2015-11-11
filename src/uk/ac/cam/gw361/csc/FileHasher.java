package uk.ac.cam.gw361.csc;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by gellert on 10/11/2015.
 */
public class FileHasher {
    public static BigInteger hashFile(String name) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            BufferedInputStream is = new BufferedInputStream(new FileInputStream(name));
            int n = 0;
            byte[] buffer = new byte[8192];
            while (n != -1) {
                n = is.read(buffer);
                if (n > 0) {
                    digest.update(buffer, 0, n);
                }
            }
            return new BigInteger(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("No such algorithm");
        }
    }
}
