package uk.ac.cam.gw361.csc.analysis;

/**
 * Created by gellert on 17/12/2015.
 */

import uk.ac.cam.gw361.csc.dht.DhtClient;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.PeerManager;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Random;
import java.util.Scanner;

public class Simulation {
    static LinkedList<Integer> numberPool = new LinkedList<>();
    static LinkedList<Integer> runningPool = new LinkedList<>();
    static final Boolean lock = false;
    static boolean isRunning = true;
    static Process[] running = new Process[100];
    static int alive = 0;
    static int startPort = 8000;
    static int max = 100;
    static Integer seed = null;
    static long mtbf = 100000;  // mtbf per node in ms
    static String localHost;
    static String hostEnd;
    static String rateLimitArg = "ratelimit=0";
    static boolean freshStart = true;

    public static void main(String[] args) {
        // set timeouts
        System.setProperty("sun.rmi.transport.proxy.connectTimeout", "1000");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "1000");
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "1000");
        try {
            localHost = InetAddress.getLocalHost().getHostAddress();
            hostEnd = localHost.substring(localHost.lastIndexOf(".") + 1);
            System.out.println(hostEnd);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            localHost = "localhost";
            hostEnd = localHost;
        }

        // create output logs directory
        File myFolder = new File("./log");
        if (!myFolder.exists()) {
            System.out.println("creating directory log");
            if (!myFolder.mkdir())
                System.err.println("creating folder failed");
        }

        String path = "/Users/gellert/src/csc/out/production/csc/";
        boolean nodel = true;
        String init = null;

        for (String arg : args) {
            if (arg.equals("del"))
                nodel = false;
            else if (arg.startsWith("nofreshstart"))
                freshStart = false;
            else if (arg.startsWith("path="))
                path = arg.substring("path=".length());
            else if (arg.startsWith("init="))
                init = arg.substring("init=".length());
            else if (arg.startsWith("startPort="))
                startPort = Integer.parseInt(arg.substring("startPort=".length()));
            else if (arg.startsWith("max="))
                max = Integer.parseInt(arg.substring("max=".length()));
            else if (arg.startsWith("mtbf="))
                mtbf = Long.parseLong(arg.substring("mtbf=".length()));
            else if (arg.startsWith("seed="))
                seed = Integer.parseInt(arg.substring("seed=".length()));
            else if (arg.startsWith("ratelimit="))
                rateLimitArg = arg;
            else
                System.err.println("Unrecognised command: " + arg);
        }

        if (!nodel) {
            // clean up
            File dir = new File(path + "storage");
            deleteFile(dir);
        }

        Thread commandReader = new SimulationCommandReader();
        commandReader.start();

        startSimulation(seed, path, init);
    }

    static void deleteFile(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                deleteFile(c);
        }
        f.delete();
    }

    private static void startSimulation(Integer seed, String path, String init) {
        int min = 1;

        Random random;
        if (seed != null)
            random = new Random(seed);
        else
            random = new Random();

        // if no init provided, spawn a static LocalPeer to connect to
        if (init == null)
            running[0] = (startOne(0, -1, path));

        for (int i=1; i<=max; i++)
            numberPool.add(i);

        double nextDeath = 0;
        double nextBirth = 0;

        while (true) {
            if (nextDeath <= 0 && alive > min) {
                Double r1 = random.nextDouble();
                nextDeath = mtbf * Math.log(1 / r1);
            }
            if (nextBirth <= 0 && alive < max) {
                Double r2 = random.nextDouble();
                nextBirth = mtbf * Math.log(1 / r2);
            }

            if (nextDeath >= 0 && alive > min)
                nextDeath /= alive;
            if (nextBirth >= 0 && alive < max)
                nextBirth /= (max - alive);

            double waitmin = nextDeath;
            if (waitmin <= 0 || waitmin > nextBirth)
                waitmin = nextBirth;

            try { Thread.sleep((long)waitmin); }
            catch (InterruptedException e) { e.printStackTrace(); }

            nextBirth -= waitmin;
            nextDeath -= waitmin;

            if (nextDeath >= 0 && alive > min)
                nextDeath *= alive;
            if (nextBirth >= 0 && alive < max)
                nextBirth *= (max - alive);

            synchronized (lock) {
                if (!isRunning)
                    try { Thread.sleep(1000); } catch (InterruptedException e) { }
                else {
                    if (nextBirth == 0 && alive < max) {
                        Double r3 = random.nextDouble() * (numberPool.size());
                        int index = r3.intValue();
                        int port = numberPool.get(index);
                        numberPool.remove(index);
                        runningPool.add(port);

                        if (init == null)
                            running[port] = (startOne(port, 0, path));
                        else
                            running[port] = (startOne(port, init, path));

                        System.out.println("Starting " + port + " alive: " + alive);
                    }
                    if (nextDeath == 0 && alive > min) {
                        Double r3 = random.nextDouble() * runningPool.size();
                        int index = r3.intValue();
                        int port = runningPool.get(index);
                        runningPool.remove(index);
                        numberPool.add(port);

                        endProcess(running[port]);
                        System.out.println("Killed process " + port + " alive: " + alive);
                    }
                }
            }
        }
    }

    static Process startOne(Integer i, Integer connectTo, String path) {
        ProcessBuilder pb = new ProcessBuilder("java", "uk.ac.cam.gw361.csc.Server",
                "username=" + hostEnd + "-" + (startPort + i) + ":" + (startPort + i),
                "host=" + localHost + ":" + (startPort + connectTo), rateLimitArg,
                freshStart ? "freshStart" : "");
        pb.redirectOutput(new File("./log/" + i + ".out"));
        return doStart(pb, path);
    }

    static Process startOne(Integer i, String connectAddress, String path) {
        ProcessBuilder pb = new ProcessBuilder("java", "uk.ac.cam.gw361.csc.Server",
                "username=" + hostEnd + "-" + (startPort + i) + ":" + (startPort + i),
                "host=" + connectAddress, rateLimitArg,
                freshStart ? "freshStart" : "");
        pb.redirectOutput(new File("./log/" + i + ".out"));
        return doStart(pb, path);
    }

    static Process doStart(ProcessBuilder pb, String path) {
        Process ret = null;
        //pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.directory(new File(path));
        try {
            ret = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        alive++;
        return ret;
    }

    static synchronized void endProcess(int index) {
        endProcess(running[index]);
    }

    static synchronized void endProcess(Process process) {
        process.destroyForcibly();
        alive--;
    }
}


class SimulationCommandReader extends Thread {
    // create LocalPeer and DhtClient only for running queries
    DhtClient client;
    public void run() {
        client = new DhtClient(PeerManager.spawnPeer(
                Simulation.hostEnd + "-simulator:9999", 1000000000));

        Scanner scanner = new Scanner(System.in);
        while (true) {
            String readStr = scanner.nextLine();
            try {
                if (readStr.equals("end")) {
                    synchronized (Simulation.lock) {
                        Simulation.isRunning = false;
                    }
                    for (Process p : Simulation.running)
                        if (p != null) Simulation.endProcess(p);
                } else if (readStr.equals("suspend")) {
                    synchronized (Simulation.lock) {
                        Simulation.isRunning = false;
                    }
                } else if (readStr.equals("continue")) {
                    synchronized (Simulation.lock) {
                        Simulation.isRunning = true;
                    }
                } else if (readStr.startsWith("kill ")) {
                    readStr = readStr.substring("kill ".length());
                    int index = Integer.parseInt(readStr);
                    Simulation.endProcess(index);
                } else if (readStr.startsWith("all ")) {
                    readStr = readStr.substring("all ".length());
                    synchronized (Simulation.lock) {
                        for (int index : Simulation.runningPool) {
                            int connectPort = Simulation.startPort + index;
                            System.out.println(connectPort);
                            System.out.println(queryPort(connectPort, readStr));
                        }
                    }
                } else if (readStr.contains(" ")) {
                    String[] splitStr = readStr.split(" ", 2);
                    int connectPort = Integer.parseInt(splitStr[0]);
                    System.out.println(queryPort(connectPort, splitStr[1]));
                } else System.out.println("Unrecognised command: " + readStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String queryPort(int connectPort, String query) {
        DhtPeerAddress toConnect = new DhtPeerAddress(null, "localhost", connectPort, null);
        try {
            return client.query(toConnect, query);
        } catch (IOException e) {
            return e.toString();
        }
    }
}
