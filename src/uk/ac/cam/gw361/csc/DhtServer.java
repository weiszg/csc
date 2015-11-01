package uk.ac.cam.gw361.csc;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;


/**
 * Created by gellert on 01/11/2015.
 */
public class DhtServer implements DhtComm {
    public String sayHello() {
        return "Hello, world!";
    }

    public static void startServer() {
        try {
            DhtServer obj = new DhtServer();
            DhtComm stub = (DhtComm) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.createRegistry(DhtComm.registryPort);
            registry.bind("DhtComm", stub);

            System.err.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

}
