package uk.ac.cam.gw361.csc.analysis;

import uk.ac.cam.gw361.csc.Client;
import uk.ac.cam.gw361.csc.Server;
import uk.ac.cam.gw361.csc.dht.TransferObserver;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by gellert on 22/03/2016.
 */
public class PerfTest implements TransferObserver {
    long startTime;
    long medTime;
    long endTime;
    Reporter reporter = new Reporter("perftest.csv");

    public static void main(String[] args) {
        int serverArgCt = Integer.parseInt(args[0]);
        String[] serverArgs = Arrays.copyOfRange(args, 1, serverArgCt + 1);
        String[] clientArgs = Arrays.copyOfRange(args, serverArgCt + 1, args.length);

        System.out.print("Servers: ");
        for (String s : serverArgs)
            System.out.print(s + " ");
        System.out.println();

        System.out.print("Client args: ");
        for (String s : clientArgs)
            System.out.print(s + " ");
        System.out.println();

        Supervisor.timeBetweenRefresh = 10000000;
        Supervisor.refresh();
        Client.main(clientArgs);
        Server.localPeer.getClient().disableCaching = true;
        Supervisor supervisor = new Supervisor(serverArgs);
        supervisor.start();

        System.out.println("Simulation starting in 3s");
        waitMillis(3000);

        PerfTest perfTest = new PerfTest();
        try {
            perfTest.startSimul();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void waitMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) { }
    }

    void createFiles(long size, String fileName) throws IOException {
        ProcessBuilder pb = new ProcessBuilder("dd", "if=/dev/urandom",
                "of=" + fileName, "bs=1k", "count=" + size/1000);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        try { p.waitFor(); }
        catch (InterruptedException e) { throw new IOException("Interrupted"); }
    }

    void startSimul() throws IOException {
        Server.localPeer.subscribeTransfers(this);
        for (long size = 1000 * 1000; size <= 100 * 1000 * 1000; size *= 10) {
            for (int bs = 100*1000; bs<=1000*1000; bs *= 10) {
                if (bs <= size) {
                    System.out.println("Size=" + size + ", blocksize=" + bs);
                    doSimul(size, bs);
                }
            }
        }
    }


    void doSimul(long size, int bs) throws IOException {
        long ultime, dltime;
        String fileName = "random-" + size/1000 + "kb";

        System.out.println("Cleaning up...");
        System.out.println(Supervisor.executeQuery("rmall"));

        System.out.println("Waiting 1s");
        waitMillis(1000);
        System.out.println("Starting test with size " + size / 1000 + " kb");

        System.out.println("Creating test file");
        createFiles(size, fileName);

        System.out.println("Uploading test file");
        startTime = System.currentTimeMillis();
        Server.localPeer.publishFile(fileName, bs);
        awaitTransfer();
        endTime = System.currentTimeMillis();
        System.out.println("Took " + (endTime - startTime) + " ms");
        ultime = endTime - startTime;


        System.out.println("Waiting for well-replication");
        awaitReplication();

        System.out.println("Downloading test file list");
        startTime = System.currentTimeMillis();
        Server.localPeer.getFileList("client", "./keys/" + "client" + "-public.key");
        awaitTransfer();
        medTime = System.currentTimeMillis();
        System.out.println("Downloading test file");
        Server.localPeer.getFile(fileName);
        awaitTransfer();
        endTime = System.currentTimeMillis();
        System.out.println("Took " + (endTime - startTime) + " ms" +
                ", of which retrieving file list took " + (medTime - startTime));
        dltime = endTime - startTime;

        reporter.report(new String[]{String.valueOf(size/1000),
                String.valueOf(bs/1000),
                String.valueOf(ultime),
                String.valueOf(dltime),
                String.valueOf((double)size / ultime / 1000),
                String.valueOf((double)size / dltime / 1000)});
        reporter.flush();
    }

    void awaitTransfer() throws IOException {
        synchronized (this) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new IOException("Interrupted");
            }
        }
    }

    void awaitReplication() throws IOException {
        do {
            waitMillis(1000);
            Supervisor.refresh();
            System.out.print("(" + Supervisor.replicatedct + " / " +
                    Supervisor.globalFiles.size() + ")");
        } while (Supervisor.replicatedct != Supervisor.globalFiles.size());
    }

    @Override
    public void notifyFinished(String transfer) {
        System.out.println("Transfer " + transfer + " finished.");
        synchronized (this) {
            notifyAll();
        }
    }
}
