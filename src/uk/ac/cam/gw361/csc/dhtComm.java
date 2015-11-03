package uk.ac.cam.gw361.csc;

import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;

/**
 * Created by gellert on 01/11/2015.
 */
public interface DhtComm extends Remote {
    DhtPeerAddress lookup(DhtPeerAddress source, BigInteger target) throws RemoteException;
    NeighbourState getNeighbourState(DhtPeerAddress source) throws RemoteException;
}
