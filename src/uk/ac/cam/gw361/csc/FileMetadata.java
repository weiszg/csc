package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gellert on 13/12/2015.
 */
public class FileMetadata implements CscData, Serializable {
    private Map<FileInterval, BigInteger> hashTree = new HashMap<>();


    FileMetadata(String file) throws IOException {
        File f = new File(file);

        for (long length = 1; length/2 < f.length(); length *= 2) {
            FileInputStream fis = new FileInputStream(file);
            try {
                BufferedInputStream bis = new BufferedInputStream(fis);
                for (long pos = 0; pos < f.length(); pos += length * FileInterval.blockSize) {
                    BigInteger nextHash = FileHasher.hashFile(bis, length * FileInterval.blockSize);
                    hashTree.put(new FileInterval(pos / FileInterval.blockSize, length), nextHash);
                }
            } finally {
                fis.close();
            }
        }
    }
}

class FileInterval implements Comparable<FileInterval> {
    static final long blockSize = 1024*1024; //1MiB
    Long start, length;

    FileInterval(long start, long length) {
        this.start = start;
        this.length = length;
    }

    @Override
    public int compareTo(FileInterval other){
        int lengthComp = length.compareTo(other.length);
        if (lengthComp == 0) return start.compareTo(other.start);
        else return lengthComp;
    }
}
