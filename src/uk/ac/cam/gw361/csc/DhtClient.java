package uk.ac.cam.gw361.csc;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtClient {
    public static void connect(String host) {
        int port = 8000;
        if (host.contains(":")) {
            port = Integer.parseInt(host.split(":")[1]);
            host = host.split(":")[0];
        }
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            DhtComm stub = (DhtComm) registry.lookup("DhtComm");
            String response = stub.sayHello();
            System.out.println("response: " + response);
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }

}
