package uk.ac.cam.gw361.csc.analysis;

import uk.ac.cam.gw361.csc.storage.DhtFile;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by gellert on 15/03/2016.
 */
public class StateReport implements Serializable {
    HashMap<BigInteger, Integer> replicationDegree;
    HashSet<DhtFile> filesStored;
    int predecessorLength, successorLength;
    long lastStabilised;
    float upspeed, downspeed;

    public StateReport(HashSet<DhtFile> filesStored,
                       HashMap<BigInteger, Integer> replicationDegree,
                       int predecessorLength, int successorLength,
                       long lastStabilised, float upspeed, float downspeed) {
        this.filesStored = filesStored;
        this.replicationDegree = replicationDegree;
        this.predecessorLength = predecessorLength;
        this.successorLength = successorLength;
        this.lastStabilised = lastStabilised;
        this.upspeed = Float.isNaN(upspeed) ? 0 : upspeed;
        this.downspeed = Float.isNaN(downspeed) ? 0 : downspeed;
    }
}
