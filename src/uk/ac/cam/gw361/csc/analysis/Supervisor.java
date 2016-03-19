package uk.ac.cam.gw361.csc.analysis;

import uk.ac.cam.gw361.csc.dht.DhtComm;
import uk.ac.cam.gw361.csc.dht.TimedRMISocketFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by gellert on 14/03/2016.
 */
public class Supervisor {
    private static LinkedList<String> servers = new LinkedList<>();
    private static HashMap<String, DhtComm> connections = new HashMap<>();
    private static TreeMap<String, StateReport> state = new TreeMap<>();
    private static HashMap<BigInteger, GlobalFileData> globalFiles = new HashMap<>();
    private static int doubleOwnersCount = 0;  // how many files have multiple owners

    public static void main(String[] args) {
        try {
            RMISocketFactory.setSocketFactory(new TimedRMISocketFactory());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        for (String arg : args) {
            LinkedList<String> newServers = new LinkedList<>();
            if (arg.contains("-")) {
                String[] input = arg.split("-");
                String[] first = input[0].split(":");
                Integer startPort = Integer.parseInt(first[1]);
                Integer endPort = Integer.parseInt(input[1]);
                for (int i=startPort; i<=endPort; i++) {
                    newServers.add(first[0] + ":" + i);
                }
            } else {
                newServers.add(arg);
            }

            for (String server : newServers)
                servers.add(server);
        }

        while (true) {
            establishConnections();
            refreshState();
            printLines();
            printState();
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {}
        }
    }

    private static void printState() {
        // print individual peer status
        int alive = 0;
        for (Map.Entry<String, StateReport> entry : state.entrySet()) {
            Long age = System.nanoTime() / 1000000 - entry.getValue().lastStabilised;
            // if too old, assume dead
            if (age <= 10000) {
                System.out.println(entry.getKey() + " ---" +
                        " pred: " + entry.getValue().predecessorLength +
                        " succ: " + entry.getValue().successorLength +
                        " age (ms): " + age.toString());
                alive++;
            }
        }
        System.out.println("Peers alive: " + alive);

        // print global file status
        System.out.println("Double-owned file count: " + doubleOwnersCount);
        System.out.println("Total file count: " + globalFiles.size());
        // print individual file status
        System.out.println("File ID -> owner count * max tracked replication count, " +
                "real replication count :");
        for (Map.Entry<BigInteger, GlobalFileData> entry : globalFiles.entrySet()) {
            System.out.println(entry.getKey().toString() + " -> " +
                    entry.getValue().ownerCount + " * " +
                    entry.getValue().trackedReplicationCount + ", " +
                    entry.getValue().realReplicationCount);
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

    private static void establishConnections() {
        for (String server : servers) {
            if (!connections.containsKey(server)) {
                addConnection(server);
            }
        }
    }

    private static void refreshState() {
        doubleOwnersCount = 0;
        globalFiles.clear();
        state.clear();
        LinkedList<String> toRemove = new LinkedList<>();
        long time = System.nanoTime();

        for (Map.Entry<String, DhtComm> entry : connections.entrySet()) {
            try {
                StateReport report = entry.getValue().getStateReport();

                // adjust time delta
                report.lastStabilised = System.nanoTime() / 1000000 - report.lastStabilised;

                Long age = time / 1000000 - report.lastStabilised;
                // only use state if fresh
                if (age <= 10000) {
                    // go through each responsible file and adjust global files
                    for (Map.Entry<BigInteger, Integer> repl : report.replicationDegree.entrySet()) {
                        GlobalFileData fileData = globalFiles.getOrDefault(repl.getKey(),
                                new GlobalFileData(0, 0, 0));
                        fileData.ownerCount++;
                        if (fileData.ownerCount == 2) doubleOwnersCount++;
                        fileData.trackedReplicationCount = Math.max(
                                fileData.trackedReplicationCount, repl.getValue());
                        globalFiles.put(repl.getKey(), fileData);
                    }

                    // count total number of peers storing each file
                    for (BigInteger file : report.filesStored) {
                        GlobalFileData fileData = globalFiles.getOrDefault(file,
                                new GlobalFileData(0, 0, 0));
                        fileData.trackedReplicationCount++;
                        globalFiles.put(file, fileData);
                    }

                    state.put(entry.getKey(), report);
                }
            } catch (RemoteException e) {
                toRemove.add(entry.getKey());
            }
        }

        for (String remove : toRemove)
            connections.remove(remove);
    }

    private static void addConnection(String server) {
        String[] splitServer = server.split(":");
        try {
            Registry registry = LocateRegistry.getRegistry(splitServer[0],
                    Integer.parseInt(splitServer[1]));
            DhtComm ret  = ((DhtComm) registry.lookup("DhtComm"));
            connections.put(server, ret);

        } catch (RemoteException | NotBoundException e) {
            // System.out.println("failed to connect to " + server + ": " + e.toString());
        }
    }
}

class GlobalFileData {
    int ownerCount, trackedReplicationCount, realReplicationCount;
    GlobalFileData(int ownerCount, int trackedReplicationCount, int realReplicationCount) {
        this.ownerCount = ownerCount;
        this.trackedReplicationCount = trackedReplicationCount;
        this.realReplicationCount = realReplicationCount;
    }
}
