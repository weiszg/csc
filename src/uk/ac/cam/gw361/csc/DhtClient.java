package uk.ac.cam.gw361.csc;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtClient {
    public static void main(String[] args) {

        String host = (args.length < 1) ? null : args[0];
        try {
            Registry registry = LocateRegistry.getRegistry(host, DhtClien.registryPort);
            DhtComm stub = (DhtComm) registry.lookup("DhtComm");
            String response = stub.sayHello();
            System.out.println("response: " + response);
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }

}
