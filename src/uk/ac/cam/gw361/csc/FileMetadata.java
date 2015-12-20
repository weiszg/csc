package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by gellert on 13/12/2015.
 */
public class FileMetadata implements CscData, Serializable {
    static final int blockSize = 1024*1024;
    private ArrayList<BigInteger> hashes;
    public final long length;
    public final int blocks;

    FileMetadata(String file) throws IOException {
        File f = new File(file);
        length = f.length();
        blocks = (int)((length+1) / blockSize);
        hashes = new ArrayList<>(blocks);

        FileInputStream fis = new FileInputStream(file);
        try {
            BufferedInputStream bis = new BufferedInputStream(fis);
            for (int i = 0; i < blocks; i++) {
                BigInteger nextHash = FileHasher.hashFile(bis, blockSize);
                hashes.set(i, nextHash);
            }
        } finally {
            fis.close();
        }
    }

    public TreeMap<Integer, BigInteger> getChunks() {
        TreeMap<Integer, BigInteger> result = new TreeMap<>();
        for (int i=0; i<blocks; i++)
            result.put(i, hashes.get(i));
        return result;
    }

    public TreeMap<Integer, BigInteger> getDiff(FileMetadata other) {
        TreeMap<Integer, BigInteger> result = new TreeMap<>();
        for (int i=0; i<blocks; i++)
            if (i >= other.blocks || !hashes.get(i).equals(other.hashes.get(i)))
                result.put(i, hashes.get(i));
        return result;
    }
}
