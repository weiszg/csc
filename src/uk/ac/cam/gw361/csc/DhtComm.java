package uk.ac.cam.gw361.csc;

import java.io.IOError;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

/**
 * Created by gellert on 01/11/2015.
 */
public interface DhtComm extends Remote {
    DhtPeerAddress lookup(DhtPeerAddress source, BigInteger target) throws IOException;
    NeighbourState getNeighbourState(DhtPeerAddress source) throws RemoteException;
    Long upload(DhtPeerAddress source, Integer port, BigInteger file) throws IOException;
    void download(DhtPeerAddress source, Integer port, BigInteger file) throws IOException;
    Map<BigInteger, Long> getRange(DhtPeerAddress source, BigInteger from, BigInteger to) throws RemoteException;
    Boolean isAlive(DhtPeerAddress source) throws RemoteException;
}
