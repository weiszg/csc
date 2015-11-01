package uk.ac.cam.gw361.csc;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by gellert on 01/11/2015.
 */
public interface DhtComm extends Remote {
    int registryPort = 8000;
    String sayHello() throws RemoteException;
}
