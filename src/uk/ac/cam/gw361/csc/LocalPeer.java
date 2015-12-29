package uk.ac.cam.gw361.csc;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.util.*;

/**
 * Created by gellert on 24/10/2015.
 */

//todo: diff download
//todo: resume failed file up/downloads
public class LocalPeer {
    final String userName;
    final String fileListPath;
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

    private PrivateKey privateKey;
    private PublicKey publicKey;
    FileList fileList;
    private FileList lastQueriedFileList;

    public LocalPeer(String userName, long stabiliseInterval) {
        int port = 8000;
        if (userName.contains(":")) {
            port = Integer.parseInt(userName.split(":")[1]);
            userName = userName.split(":")[0];
        }

        this.userName = userName;
        userID = Hasher.hashString(userName);
        localAddress = new DhtPeerAddress(userID, "localhost", port, userID);
        neighbourState = new NeighbourState(localAddress);

        dhtStore = new DhtStore(this);
        dhtClient = new DhtClient(this);
        dhtServer = new DhtServer(this, port);
        dhtServer.startServer();

        fileListPath = "./storage/" + userName + "/" + "FileList";
        loadKeys();

        stabiliser = new Stabiliser(this, stabiliseInterval);
        localAddress.print(System.out, "Started: ");
    }

    private void loadKeys() {
        // create directory
        File myFolder = new File("./keys");
        if (!myFolder.exists()) {
            System.out.println("creating directory keys");
            if (!myFolder.mkdir())
                System.err.println("creating folder failed");
        }
        // load keypair
        KeyPair keyPair = FileList.initKeys("./keys/" + userName + "-");
        privateKey = keyPair.getPrivate();
        publicKey = keyPair.getPublic();
        fileList = FileList.loadOrCreate(fileListPath, publicKey);
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
                file, true, null);
    }

    public DhtTransfer publishEntity(String file) throws IOException {
        return dhtClient.upload(file, null);
    }

    public DhtTransfer getFileList(String user, String publicKeyLoc) throws IOException {
        BigInteger ID = Hasher.hashString(user);
        PublicKey publicKey = null;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(publicKeyLoc))) {
            Object myobj = ois.readObject();
            if (!(myobj instanceof PublicKey)) {
                System.err.println("Not a valid public key");
                return null;
            }
            publicKey = (PublicKey) myobj;
            FileDownloadContinuation.createDir();
            String fileName = FileListDownloadContinuation.transferDir + ID.toString() + ".files";
            return dhtClient.download(fileName, ID, false,
                    new FileListDownloadContinuation(fileName, publicKey));
        } catch (ClassNotFoundException e) {
            System.err.println(e.toString());
            return null;
        }
    }

    public DhtTransfer getFile(String fileName) throws IOException {
        if (lastQueriedFileList == null || lastQueriedFileList.get(fileName) == null) {
            System.err.println("File not found");
            return null;
        }
        BigInteger fileMeta = lastQueriedFileList.get(fileName);
        return getFile(fileName, fileMeta);
    }

    public DhtTransfer getFile(String fileName, BigInteger fileMeta) throws IOException {
        FileDownloadContinuation.createDir();
        return dhtClient.download(FileDownloadContinuation.transferDir + fileName + ".meta",
                fileMeta, true, new FileDownloadContinuation(fileName));
    }

    public DhtTransfer publishFile(String fileName) throws IOException {
        FileUploadContinuation.createDir();
        Path p = Paths.get(fileName);
        String lastName = p.getFileName().toString();
        FileMetadata meta = new FileMetadata(fileName, lastName);

        String metaLocation = FileUploadContinuation.transferDir + lastName + ".metadata";

        try (ObjectOutputStream ous = new ObjectOutputStream(new FileOutputStream(metaLocation))) {
            ous.writeObject(meta);
        }

        FileUploadContinuation continuation = new FileUploadContinuation(fileName, meta);
        return dhtClient.upload(FileUploadContinuation.transferDir + lastName + ".metadata",
                continuation);
    }

    void replicate(DhtFile file) throws IOException {
        List<DhtPeerAddress> predecessors = neighbourState.getPredecessors();
        for (DhtPeerAddress p : predecessors) {
             dhtClient.upload(p, file.hash, localAddress, null);
        }
    }

    synchronized void notifyTransferCompleted(DhtTransfer ft, boolean success) {
        runningTransfers.remove(ft);
    }

    synchronized String saveFileList() {
        try {
            SignedObject so = fileList.getSignedVersion(privateKey);
            ObjectOutputStream ous = new ObjectOutputStream(new FileOutputStream(fileListPath));
            ous.writeObject(so);
            ous.flush();
            ous.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileListPath;
    }

    synchronized void setLastQueriedFileList(FileList fileList) {
        lastQueriedFileList = fileList;
        System.out.println("Files:");
        for (String file : fileList.getFileList())
            System.out.println("   " + file);
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
            } else if (input.startsWith("files")) {
                input = input.substring("files ".length());
                printStream.println("getting file list for user " + input);
                getFileList(input, "./keys/" + input + "-public.key");
            } else if (input.startsWith("dl")) {
                input = input.substring("dl ".length());
                printStream.println("downloading " + input);
                getFile(input);
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
            e.printStackTrace();
        }
        printStream.flush();
        return baos.toString();
    }
}
