package uk.ac.cam.gw361.csc.analysis;

import uk.ac.cam.gw361.csc.dht.DhtComm;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gellert on 14/03/2016.
 */
public class Supervisor {
    private static HashMap<String, DhtComm> connections = new HashMap<>();
    private static HashMap<String, StateReport> state = new HashMap<>();
    private static HashMap<BigInteger, GlobalFileData> globalFiles = new HashMap<>();
    private static int doubleOwnersCount = 0;  // how many files have multiple owners

    public static void main(String[] args) {
        for (String server : args) {
            addConnection(server);
        }

        while (true) {
            refreshState();
            printLines();
            printState();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {}
        }
    }

    private static void printState() {

        // print individual peer status
        for (Map.Entry<String, StateReport> entry : state.entrySet()) {
            Long age = System.nanoTime() / 1000000 - entry.getValue().lastStabilised;
            System.out.println(entry.getKey() + " ---" +
                    " pred: " + entry.getValue().predecessorLength +
                    " succ: " + entry.getValue().successorLength +
                    " age (ms): " + age.toString());
        }

        // print global file status
        System.out.println("Double-owned file count: " + doubleOwnersCount);
        System.out.println("Total file count: " + globalFiles.size());
        // print individual file status
        System.out.println("File ID -> owner count * max replication count:");
        for (Map.Entry<BigInteger, GlobalFileData> entry : globalFiles.entrySet()) {
            System.out.println(entry.getKey().toString() + " -> " +
                    entry.getValue().ownerCount + " * " + entry.getValue().replicationCount);
        }
    }

    private static void printLines() {
        // used instead of cls to print a bunch of empty lines to clear screen
        for (int i=0; i<5; i++)
            System.out.println("");
    }

    private static void cls() {
        // clear screen
        try {
            String os = System.getProperty("os.name");
            if (os.contains("Windows")) {
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            }
            else {
                Runtime.getRuntime().exec("clear");
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void refreshState() {
        for (Map.Entry<String, DhtComm> entry : connections.entrySet()) {
            try {
                StateReport report = entry.getValue().getStateReport();

                // adjust time delta
                report.lastStabilised = System.nanoTime() / 1000000 - report.lastStabilised;

                // go through each responsible file and adjust global files
                doubleOwnersCount = 0;
                for (Map.Entry<BigInteger, Integer> repl : report.replicationDegree.entrySet()) {
                    GlobalFileData fileData = globalFiles.getOrDefault(repl.getKey(),
                            new GlobalFileData(0, 0));
                    fileData.ownerCount++;
                    if (fileData.ownerCount == 2) doubleOwnersCount++;
                    fileData.replicationCount = Math.max(
                            fileData.replicationCount, repl.getValue());
                    globalFiles.put(repl.getKey(), fileData);
                }

                state.put(entry.getKey(), report);
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

    private static void addConnection(String server) {
        String[] splitServer = server.split(":");
        try {
            Registry registry = LocateRegistry.getRegistry(splitServer[0],
                    Integer.parseInt(splitServer[1]));
            DhtComm ret  = ((DhtComm) registry.lookup("DhtComm"));
            connections.put(server, ret);

        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
    }
}

class GlobalFileData {
    int ownerCount, replicationCount;
    GlobalFileData(int ownerCount, int replicationCount) {
        this.ownerCount = ownerCount;
        this.replicationCount = replicationCount;
    }
}
