package uk.ac.cam.gw361.csc;

/**
 * Created by gellert on 17/12/2015.
 */

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

    public static void main(String[] args) {
        // set timeouts
        System.setProperty("sun.rmi.transport.proxy.connectTimeout", "1000");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "1000");
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "1000");

        // create output logs directory
        File myFolder = new File("./log");
        if (!myFolder.exists()) {
            System.out.println("creating directory log");
            if (!myFolder.mkdir())
                System.err.println("creating folder failed");
        }

        String seed;
        String path = "/Users/gellert/src/csc/out/production/csc/";
        boolean nodel = false;
        String init = null;
        Scanner scanner = new Scanner(System.in);
        System.out.println("seed:");
        seed = scanner.next();

        for (String arg : args) {
            if (arg.equals("nodel"))
                nodel = true;
            if (arg.startsWith("path="))
                path = arg.substring("path=".length());
            if (arg.startsWith("init="))
                init = arg.substring("init=".length());
            if (arg.startsWith("startPort="))
                startPort = Integer.parseInt(arg.substring("startPort=".length()));
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

    private static void startSimulation(String seed, String path, String init) {
        int min = 1;
        int max = 100;
        double scale = 500000;

        Random random = new Random(Long.parseLong(seed));

        if (init != null)
            running[0] = (startOne(0, init, path));
        else
            running[0] = (startOne(0, 0, path));

        for (int i=1; i<max; i++)
            numberPool.add(i);

        double nextDeath = 0;
        double nextBirth = 0;

        while (true) {
            if (nextDeath <= 0 && alive > min) {
                Double r1 = random.nextDouble();
                nextDeath = scale * Math.log(1 / r1);
            }
            if (nextBirth <= 0 && alive < max) {
                Double r2 = random.nextDouble();
                nextBirth = scale * Math.log(1 / r2);
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
                    if (nextBirth == 0) {
                        Double r3 = random.nextDouble() * (numberPool.size());
                        int index = r3.intValue();
                        int port = numberPool.get(index);
                        numberPool.remove(index);
                        runningPool.add(port);

                        System.out.println("Starting " + port + " alive: " + alive);
                        running[port] = (startOne(port, 0, path));
                    }
                    if (nextDeath == 0) {
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
        try {
            ProcessBuilder pb = new ProcessBuilder("java", "uk.ac.cam.gw361.csc.Main",
                    (startPort + i) + ":" + (startPort + i),
                    InetAddress.getLocalHost().getHostAddress() + ":"
                    + (startPort + connectTo));
            pb.redirectOutput(new File("./log/" + i + ".out"));
            return doStart(pb, path);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
    }

    static Process startOne(Integer i, String connectTo, String path) {
        ProcessBuilder pb = new ProcessBuilder("java", "uk.ac.cam.gw361.csc.Main",
                (startPort + i) + ":" + (startPort + i), connectTo);
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
        } catch ( IOException e ) {
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
    DhtClient client = new DhtClient(new LocalPeer("simulator:9999", 1000000));
    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String readStr = scanner.nextLine();
            try {
                if (readStr.equals("end")) {
                    synchronized (Simulation.lock) {
                        Simulation.isRunning = false;
                    }
                    for (Process p : Simulation.running)
                        Simulation.endProcess(p);
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
