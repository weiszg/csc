package uk.ac.cam.gw361.csc.analysis;

import uk.ac.cam.gw361.csc.dht.DhtComm;
import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.dht.NeighbourState;
import uk.ac.cam.gw361.csc.dht.TimedRMISocketFactory;
import uk.ac.cam.gw361.csc.storage.DhtFile;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.util.*;

/**
 * Created by gellert on 14/03/2016.
 */
public class Supervisor extends Thread {
    private static LinkedList<String> servers = new LinkedList<>();
    private static final HashMap<String, DhtComm> connections = new HashMap<>();
    private static TreeMap<String, StateReport> state = new TreeMap<>();
    static HashMap<BigInteger, GlobalFileData> globalFiles = new HashMap<>();
    private static int doubleOwnersCount = 0;  // how many files have multiple owners
    private static int fosterct = 0;  // how many files have no owners
    static int replicatedct = 0;  // how many files are well-replicated
    static int overReplicatedct = 0;  // how many files are over-replicated
    private static int births = 0;  // how many new peers
    private static int deaths = 0;  // how many peers disappeared
    private static Reporter reporter = new Reporter("filect.csv");
    private static final boolean printFiles = false;
    private static final boolean debugDoubleOwned = false;
    public static long timeBetweenRefresh = 500;
    static float kbpsUp = 0, kbpsDown = 0;

    public static void main(String[] args) {
        try {
            RMISocketFactory.setSocketFactory(new TimedRMISocketFactory());
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        Supervisor supervisor = new Supervisor(args);
        supervisor.start();

        CommandReader commandReader = new CommandReader();
        commandReader.start();
    }

    Supervisor(String[] serverAddresses) {
        for (String arg : serverAddresses) {
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
    }

    public void run() {
        while (true) {
            synchronized (connections) {
                refresh();
                printLines();
                printState();
            }
            try {
                Thread.sleep(timeBetweenRefresh);
            } catch (InterruptedException ie) {}
        }
    }

    public static synchronized void refresh() {
        clearState();
        establishConnections();
        refreshState();
    }

    private static void printState() {
        // print individual peer status
        int alive = 0;
        int destabilisedct = 0;

        for (Map.Entry<String, StateReport> entry : state.entrySet()) {
            Long age = System.nanoTime() / 1000000 - entry.getValue().lastStabilised;
            // if too old, assume dead
            if (age <= 10000) {
                System.out.println(entry.getKey() + " ---" +
                        " pred: " + entry.getValue().predecessorLength +
                        " succ: " + entry.getValue().successorLength +
                        " age (ms): " + age.toString());
                alive++;
                if (entry.getValue().predecessorLength < NeighbourState.k ||
                        entry.getValue().successorLength < NeighbourState.k)
                    destabilisedct++;
            }
        }

        if (printFiles) {
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

        System.out.println("Peers alive: " + alive);
        System.out.println("Peers with destabilised NeighbourState: " + destabilisedct);

        // print global file status
        System.out.println("Total file count: " + globalFiles.size());
        System.out.println("Well-replicated file count: " + replicatedct);
        System.out.println("Over-replicated file count: " + overReplicatedct);
        System.out.println("Foster files count: " + fosterct);
        System.out.println("Double-owned file count: " + doubleOwnersCount);
        System.out.println("Births / deaths: " + births + " / " + deaths);
        System.out.println("Aggregate up / down kbps: " + kbpsUp + " / " + kbpsDown);

        reporter.report(new String[]{String.valueOf(System.currentTimeMillis()),
                String.valueOf(globalFiles.size()),
                String.valueOf(fosterct),
                String.valueOf(alive),
                String.valueOf(births),
                String.valueOf(deaths)});
        reporter.flush();
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

    private static void clearState() {
        doubleOwnersCount = 0;
        globalFiles.clear();
        state.clear();
        births = 0;
        deaths = 0;
        kbpsUp = 0;
        kbpsDown = 0;
    }

    private static void refreshState() {
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
                        GlobalFileData fileData = globalFiles.get(repl.getKey());
                        if (fileData == null) fileData = new GlobalFileData(0, 0, 0);

                        fileData.ownerCount++;
                        fileData.owners.add(entry.getKey());
                        if (fileData.ownerCount == 2) {
                            doubleOwnersCount++;
                            if (debugDoubleOwned)
                                System.out.println("file " + repl.getKey() + " double-owned by " +
                            fileData.owners.get(0) + " and " + fileData.owners.get(1));
                        }
                        fileData.trackedReplicationCount = Math.max(
                                fileData.trackedReplicationCount, repl.getValue());
                        globalFiles.put(repl.getKey(), fileData);
                    }

                    // count total number of peers storing each file
                    for (DhtFile file : report.filesStored) {
                        GlobalFileData fileData = globalFiles.get(file.hash);
                        if (fileData == null) fileData = new GlobalFileData(0, 0, 0);

                        fileData.realReplicationCount++;
                        globalFiles.put(file.hash, fileData);
                    }

                    kbpsUp += report.upspeed;
                    kbpsDown += report.downspeed;

                    state.put(entry.getKey(), report);
                }
            } catch (RemoteException e) {
                toRemove.add(entry.getKey());
                //System.out.println(e.toString());
                deaths++;
            }
        }

        fosterct = 0;
        replicatedct = 0;
        overReplicatedct = 0;
        for (Map.Entry<BigInteger, GlobalFileData> entry : globalFiles.entrySet()) {
            if (entry.getValue().ownerCount == 0) {
                /*for (Map.Entry<String, StateReport> report : state.entrySet()) {
                    for (DhtFile file : report.getValue().filesStored)
                        if (file.hash.equals(entry.getKey())) {
                            System.err.println("Error at " + report.getKey() + ", thinks owner=" +
                                    file.owner.getConnectAddress());
                        }
                }*/
                fosterct++;
            }
            if (entry.getValue().realReplicationCount >= NeighbourState.k)
                replicatedct++;
            if (entry.getValue().realReplicationCount > NeighbourState.k)
                overReplicatedct++;
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
            births++;

        } catch (RemoteException | NotBoundException e) {
            //System.out.println("addConnection " + server + " " + e.toString());
            // System.out.println("failed to connect to " + server + ": " + e.toString());
        }
    }

    static String executeQuery(String query) {
        int success = 0;
        for (Map.Entry<String, DhtComm> entry : connections.entrySet()) {
            try {
                String result = entry.getValue().query(query);
                success++;
            } catch (IOException e) { }
        }
        return "Successfully executed on " + success + " peers";
    }

}

class GlobalFileData {
    int ownerCount, trackedReplicationCount, realReplicationCount;
    List<String> owners = new LinkedList<>();

    GlobalFileData(int ownerCount, int trackedReplicationCount, int realReplicationCount) {
        this.ownerCount = ownerCount;
        this.trackedReplicationCount = trackedReplicationCount;
        this.realReplicationCount = realReplicationCount;
    }

    void addOwner(String owner) {
        owners.add(owner);
    }
}


class CommandReader extends Thread {
    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String readStr = scanner.nextLine();
            try {
                System.out.println(Supervisor.executeQuery(readStr));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
