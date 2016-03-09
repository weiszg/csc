package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.storage.DhtFile;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by gellert on 09/03/2016.
 */
public interface CscComm extends Remote {
    DhtPeerAddress lookup(BigInteger target) throws IOException;
    Long upload(String host, Integer port, BigInteger file) throws IOException;
    Integer download(String host, Integer port, DhtFile file) throws IOException;
    Boolean isAlive() throws RemoteException;
}
