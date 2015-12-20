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
    private DhtStore dhtStore;
    public DhtStore getDhtStore() { return dhtStore; }

    private NeighbourState neighbourState;
    public synchronized NeighbourState getNeighbourState() { return neighbourState; }
    public synchronized void setNeighbourState(NeighbourState newState) {
        neighbourState = newState;
    }
    public synchronized void addRunningTransfer(DhtTransfer ft) {
        runningTransfers.add(ft);
    }
    public void stabilise() { stabiliser.stabilise(); }

    Set<DhtTransfer> runningTransfers = new HashSet<>();

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

        dhtStore = new DhtStore(this);
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

    public DhtTransfer getEntity(BigInteger file) throws IOException {
        return dhtClient.download(FileDownloadContinuation.transferDir + file.toString(),
                file, null);
    }

    public DhtTransfer publishEntity(String file) throws IOException {
        return dhtClient.upload(file, null);
    }

    public DhtTransfer getFile(BigInteger fileMeta) throws IOException {
        return dhtClient.download(FileDownloadContinuation.transferDir
                            + fileMeta.toString(), fileMeta, new FileDownloadContinuation());
    }

    public DhtTransfer publishFile(String fileName) throws IOException {
        FileMetadata meta = new FileMetadata(fileName);
        String lastName = fileName;
        if (fileName.contains("/"))
            lastName = fileName.substring(fileName.lastIndexOf("/"));
        String metaLocation = FileUploadContinuation.transferDir + lastName + ".metadata";

        try (ObjectOutputStream ous = new ObjectOutputStream(new FileOutputStream(metaLocation))) {
            ous.writeObject(meta);
        }

        FileUploadContinuation continuation = new FileUploadContinuation(fileName, meta);
        return dhtClient.upload(fileName, continuation);
    }

    void replicate(BigInteger file) throws IOException {
        List<DhtPeerAddress> predecessors = neighbourState.getPredecessors();
        for (DhtPeerAddress p : predecessors) {
             dhtClient.upload(p, file, localAddress, null);
        }
    }

    synchronized void notifyTransferCompleted(DhtTransfer ft, boolean success) {
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
                getDhtStore().print(printStream, "");
            } else if (input.equals("stabilise")) {
                stabilise();
            } else if (input.startsWith("dle")) {
                input = input.substring("dle ".length());
                BigInteger target = new BigInteger(input);
                printStream.println("downloading " + target.toString());
                getEntity(target);
            } else if (input.startsWith("ule")) {
                input = input.substring("ule ".length());
                publishEntity(input);
                System.out.println("upload started");
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
