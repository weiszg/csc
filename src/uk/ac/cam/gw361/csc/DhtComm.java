package uk.ac.cam.gw361.csc;

import java.io.IOError;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gellert on 01/11/2015.
 */
public interface DhtComm extends Remote {
    int logKeySize = 160;  // 160-bit key sizes
    DoubleAddress nextHop(DhtPeerAddress source, BigInteger target) throws IOException;
    NeighbourState getNeighbourState(DhtPeerAddress source) throws RemoteException;
    Long upload(DhtPeerAddress source, Integer port, BigInteger file) throws IOException;
    Integer download(DhtPeerAddress source, Integer port, DhtFile file) throws IOException;
    Boolean isAlive(DhtPeerAddress source) throws RemoteException;
    Boolean checkUserID(DhtPeerAddress source, BigInteger userID) throws RemoteException;
    Map<BigInteger, Boolean> storingFiles(DhtPeerAddress source, List<DhtFile> files)
            throws IOException;
    String query(String input) throws RemoteException; // for debug purposes
}
