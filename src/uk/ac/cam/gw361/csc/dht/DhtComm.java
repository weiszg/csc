package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.analysis.StateReport;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by gellert on 01/11/2015.
 */
public interface DhtComm extends Remote {
    int logKeySize = 256;  // 256-bit key sizes

    // gets next hop towards target such that the process converges to the biggest address
    // that is strictly less than the target
    DoubleAddress nextHop(DhtPeerAddress source, BigInteger target) throws IOException;
    NeighbourState getNeighbourState(DhtPeerAddress source) throws IOException;
    TransferReply upload(DhtPeerAddress source, Integer port, BigInteger file) throws IOException;
    TransferReply download(DhtPeerAddress source, Integer port, DhtFile file) throws IOException;
    Boolean isAlive(DhtPeerAddress source) throws IOException;
    Boolean checkUserID(DhtPeerAddress source, BigInteger userID) throws IOException;
    Map<BigInteger, Boolean> storingFiles(DhtPeerAddress source, List<DhtFile> files) throws IOException;
    String query(String input) throws RemoteException; // for debug purposes
    StateReport getStateReport() throws RemoteException;
}
