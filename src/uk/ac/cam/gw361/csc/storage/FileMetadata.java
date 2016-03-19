package uk.ac.cam.gw361.csc.storage;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by gellert on 13/12/2015.
 */
public class FileMetadata implements Serializable {
    public final int blockSize;
    private ArrayList<BigInteger> hashes;
    public final long length;
    public final int blocks;
    final String fileName;

    public FileMetadata(String filePath, String fileName, int blockSize) throws IOException {
        this.blockSize = blockSize;
        this.fileName = fileName;
        File f = new File(filePath);
        length = f.length();
        blocks = (int)((length-1) / blockSize) + 1;
        hashes = new ArrayList<>(blocks);

        try (BufferedInputStream bis = new BufferedInputStream(
                new FileInputStream(filePath))) {

            for (int i = 0; i < blocks; i++) {
                BigInteger nextHash = Hasher.hashFile(bis, blockSize);
                hashes.add(nextHash);
            }
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
