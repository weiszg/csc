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
    TransferReply upload(String host, BigInteger file) throws IOException;
    TransferReply download(String host, DhtFile file) throws IOException;
    Boolean isAlive() throws RemoteException;
}
