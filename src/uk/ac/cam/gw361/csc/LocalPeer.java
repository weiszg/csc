package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by gellert on 24/10/2015.
 */
public class LocalPeer {
    final String userName;
    private final BigInteger userID;
    final DhtPeerAddress localAddress;
    private DhtServer dhtServer;
    private DhtClient dhtClient;
    private Stabiliser stabiliser;
    public DhtClient getClient() { return dhtClient; }
    public DhtServer getServer() { return dhtServer; }
    private FileStore fileStore;
    public FileStore getFileStore() { return fileStore; }

    private NeighbourState neighbourState;
    public synchronized NeighbourState getNeighbourState() { return neighbourState; }
    public synchronized void setNeighbourState(NeighbourState newState) {
        neighbourState = newState;
    }
    public synchronized void addRunningTransfer(FileTransfer ft) {
        runningTransfers.add(ft);
    }
    public void stabilise() { stabiliser.stabilise(); }

    private Set<FileTransfer> runningTransfers = new HashSet<>();

    public LocalPeer(String userName, long stabiliseInterval) {
        int port = 8000;
        if (userName.contains(":")) {
            port = Integer.parseInt(userName.split(":")[1]);
            userName = userName.split(":")[0];
        }

        this.userName = userName;
        MessageDigest cript = null;
        try {
            cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(userName.getBytes("utf8"));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        userID = new BigInteger(cript.digest());
        localAddress = new DhtPeerAddress(userID, "localhost", port, userID);
        neighbourState = new NeighbourState(localAddress);

        fileStore = new FileStore(this);
        dhtClient = new DhtClient(this);
        dhtServer = new DhtServer(this, port);
        dhtServer.startServer();
        stabiliser = new Stabiliser(this, stabiliseInterval);
        localAddress.print(System.out, "Started: ");
    }

    public synchronized void join(String remotePeerIP) {
        try {
            dhtClient.bootstrap(remotePeerIP);
            System.out.println("Connected to DHT pool");
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Failed to connect to DHT pool");
        }
    }

    public DhtPeerAddress getNextLocalHop(BigInteger target) {
        TreeSet<DhtPeerAddress> peers = neighbourState.getNeighbours();
        peers.add(localAddress);

        DhtPeerAddress next = peers.lower(
                new DhtPeerAddress(target, null, null, localAddress.getUserID()));
        if (next == null) {
            next = peers.last();
        }
        return next;
    }

    public FileTransfer getFile(BigInteger file) throws IOException {
        FileTransfer ft = null;
        DhtPeerAddress target = dhtClient.lookup(file);
        if (file != null)
            ft = dhtClient.download(target, file);
        runningTransfers.add(ft);
        return ft;
    }

    public FileTransfer publishFile(String file) throws IOException {
        BigInteger hash = FileHasher.hashFile(file);
        FileTransfer ft = null;
        DhtPeerAddress target = dhtClient.lookup(hash);
        if (file != null)
            ft = dhtClient.upload(target, hash, file, target);
        runningTransfers.add(ft);
        return ft;
    }

    void replicate(BigInteger file) throws IOException {
        List<DhtPeerAddress> predecessors = neighbourState.getPredecessors();
        FileTransfer ft = null;
        for (DhtPeerAddress p : predecessors) {
            ft = dhtClient.upload(p, file, localAddress);
            runningTransfers.add(ft);
        }
    }

    synchronized void notifyTransferCompleted(FileTransfer ft, boolean success) {
        runningTransfers.remove(ft);
    }

    void disconnect() {
        dhtServer.stopServer();
        stabiliser.disconnect();
    }

    public String executeQuery(String input) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(baos);

        try {
            if (input.equals("nb")) {
                getNeighbourState().print(printStream, "");
            } else if (input.equals("files")) {
                getFileStore().print(printStream, "");
            } else if (input.equals("stabilise")) {
                stabilise();
            } else if (input.startsWith("dl")) {
                input = input.substring("dl ".length());
                BigInteger target = new BigInteger(input);
                printStream.println("downloading " + target.toString());
                getFile(target);
            } else if (input.startsWith("ul")) {
                input = input.substring("ul ".length());
                publishFile(input);
                System.out.println("upload started");
            } else if (input.contains(" ")) {
                String[] splitStr = input.split(" ", 2);
                int connectPort = Integer.parseInt(splitStr[0]);
                DhtPeerAddress toConnect = new DhtPeerAddress(null, "localhost", connectPort, null);
                System.out.println(getClient().query(toConnect, splitStr[1]));
            } else System.out.println("Unrecognised command: " + input);
        } catch (IOException e) {
            printStream.println(e.toString());
        }
        printStream.flush();
        return baos.toString();
    }
}
