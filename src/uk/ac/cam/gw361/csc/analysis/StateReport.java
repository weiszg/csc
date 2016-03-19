package uk.ac.cam.gw361.csc.analysis;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by gellert on 15/03/2016.
 */
public class StateReport implements Serializable {
    HashMap<BigInteger, Integer> replicationDegree;
    HashSet<BigInteger> filesStored;
    int predecessorLength, successorLength;
    long lastStabilised;

    public StateReport(HashSet<BigInteger> filesStored,
                       HashMap<BigInteger, Integer> replicationDegree,
                       int predecessorLength, int successorLength,
                       long lastStabilised) {
        this.filesStored = filesStored;
        this.replicationDegree = replicationDegree;
        this.predecessorLength = predecessorLength;
        this.successorLength = successorLength;
        this.lastStabilised = lastStabilised;
    }
}
