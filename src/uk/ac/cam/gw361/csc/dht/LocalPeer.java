package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.storage.*;
import uk.ac.cam.gw361.csc.transfer.*;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.util.*;

/**
 * Created by gellert on 24/10/2015.
 */

public class LocalPeer {
    public final String userName;
    public boolean cscOnly;
    final String fileListPath;
    private final BigInteger userID;
    public final DhtPeerAddress localAddress;
    private DhtServer dhtServer, localDhtServer;
    private DhtClient dhtClient;
    private Stabiliser stabiliser;
    private DhtStore dhtStore;
    private TransferManager transferManager;
    private FingerState fingerState;
    private NeighbourState neighbourState;

    public DhtClient getClient() { return dhtClient; }
    public DhtServer getServer() { return localDhtServer; }
    public DhtStore getDhtStore() { return dhtStore; }
    public TransferManager getTransferManager() { return transferManager; }
    public Stabiliser getStabiliser() { return stabiliser; }

    public synchronized NeighbourState getNeighbourState() { return neighbourState; }
    public synchronized FingerState getFingerState() { return fingerState; }
    public synchronized void setNeighbourState(NeighbourState newState) {
        neighbourState = newState;
    }
    public void stabilise() { stabiliser.stabilise(); }

    private PrivateKey privateKey;
    PublicKey publicKey;
    public FileList fileList;
    private FileList lastQueriedFileList;

    public LocalPeer(String userName, long stabiliseInterval, boolean cscOnly, boolean freshStart) {
        this.cscOnly = cscOnly;
        int port = 8000;
        if (userName.contains(":")) {
            port = Integer.parseInt(userName.split(":")[1]);
            userName = userName.split(":")[0];
        }

        this.userName = userName;
        userID = Hasher.hashString(userName);
        String myHost = "localhost";
        try {
            myHost = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) { }

        localAddress = new DhtPeerAddress(userID, myHost, port, userID);
        neighbourState = new NeighbourState(localAddress);
        fingerState = new FingerState(this);

        dhtClient = new DhtClient(this);
        transferManager = new TransferManager(this);
        transferManager.start();
        fileListPath = "./storage/" + userName + "/" + "MyFileList";

        if (!cscOnly) {
            dhtStore = new DhtStore(this, !freshStart);
            dhtServer = new DhtServer(this, port);
            localDhtServer = new DhtServer(dhtServer, true);
            dhtServer.startServer();
            stabiliser = new Stabiliser(this, stabiliseInterval);
            localAddress.print(System.out, "Started: ");
        } else {
            dhtStore = new DhtStore(this, false);
            loadKeys();
        }
    }

    public boolean isCscOnly() {
        return cscOnly;
    }

    boolean isStable() { return stabiliser.isStable(); }

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
        fileList = FileList.loadOrCreate(fileListPath + ".signed", publicKey);
    }

    public synchronized void join(String remotePeerIP) {
        if (isCscOnly()) {
            int port = 8000;
            if (remotePeerIP.contains(":")) {
                port = Integer.parseInt(remotePeerIP.split(":")[1]);
                remotePeerIP = remotePeerIP.split(":")[0];
            }

            neighbourState.addNeighbour(new DhtPeerAddress(BigInteger.ONE,
                    remotePeerIP, port, BigInteger.ZERO));

            // try fetching my file list
            try {
                if (cscOnly) getFileList(userName, publicKey, true);
            } catch (IOException e) {
                System.out.println("No files uploaded yet");
            }
        } else {
            // set join information of the Stabiliser
            stabiliser.setJoin(remotePeerIP);
            stabilise();
        }
    }

    public DoubleAddress getNextLocalHop(BigInteger target) {
        DhtPeerAddress targetAddress =
                new DhtPeerAddress(target, null, null, localAddress.getUserID());
        TreeSet<DhtPeerAddress> peers = neighbourState.getNeighbours();
        if (!isCscOnly()) peers.add(localAddress);

        DhtPeerAddress nextNeighbour = peers.lower(targetAddress);
        if (nextNeighbour == null) {
            nextNeighbour = peers.last();
        }

        peers = fingerState.getFingers();
        DhtPeerAddress nextFinger = peers.lower(targetAddress);

        // check if this is any better than the neighbour information
        if (nextFinger != null && nextNeighbour.compareTo(nextFinger) > 0)
            nextFinger = null;

        return new DoubleAddress(nextNeighbour, nextFinger);
    }

    public void getEntity(BigInteger file) throws IOException {
        transferManager.download(FileDownloadContinuation.transferDir + file.toString(),
                file, true, null, true);
    }

    public void publishEntity(String file) throws IOException {
        transferManager.upload(file, null);
    }

    public void getFileList(String user, String publicKeyLoc) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(publicKeyLoc))) {
            Object myobj = ois.readObject();
            if (!(myobj instanceof PublicKey)) {
                System.err.println("Not a valid public key");
                return;
            }
            PublicKey publicKey = (PublicKey) myobj;
            getFileList(user, publicKey, false);
        } catch (ClassNotFoundException e) {
            System.err.println(e.toString());
        }
    }

    void getFileList(String user, PublicKey publicKey, boolean own)
            throws IOException {
        BigInteger ID = Hasher.hashString(user);
        FileDownloadContinuation.createDir();
        String fileName = FileListDownloadContinuation.transferDir + ID.toString() + ".files";
        // retry unless querying for own FileList (that might not exist)
        transferManager.download(fileName, ID, false,
                new FileListDownloadContinuation(fileName, publicKey, own), !own);
    }

    public void getFile(String fileName) throws IOException {
        if (lastQueriedFileList == null || lastQueriedFileList.get(fileName) == null) {
            System.err.println("File not found");
            return;
        }
        BigInteger fileMeta = lastQueriedFileList.get(fileName);
        getFile(fileName, fileMeta);
    }

    public void getFile(String fileName, BigInteger fileMeta) throws IOException {
        FileDownloadContinuation.createDir();
        transferManager.download(FileDownloadContinuation.transferDir + fileName + ".meta",
                fileMeta, true, new FileDownloadContinuation(fileName), true);
    }

    public void publishFile(String fileName) throws IOException {
        // use default blockSize of 1 MB
        publishFile(fileName, 10*1024*1024); // try 10M
    }

    public void publishFile(String fileName, int blockSize) throws IOException {
        FileUploadContinuation.createDir();
        Path p = Paths.get(fileName);
        String lastName = p.getFileName().toString();
        FileMetadata meta = new FileMetadata(fileName, lastName, blockSize);

        String metaLocation = FileUploadContinuation.transferDir + lastName + ".meta";

        try (ObjectOutputStream ous = new ObjectOutputStream(new FileOutputStream(metaLocation))) {
            ous.writeObject(meta);
        }

        FileUploadContinuation continuation = new FileUploadContinuation(fileName, meta);
        transferManager.upload(FileUploadContinuation.transferDir + lastName + ".meta",
                continuation);
    }

    void replicate(DhtFile file) throws IOException {
        List<DhtPeerAddress> predecessors = neighbourState.getPredecessors();
        for (DhtPeerAddress p : predecessors) {
             transferManager.upload(p, file.hash, new InternalUploadContinuation(), true);
        }
    }


    public synchronized String saveFileList() {
        try {
            SignedFileList sf = fileList.getSignedVersion(privateKey);
            ObjectOutputStream ous = new ObjectOutputStream(
                    new FileOutputStream(fileListPath + ".signed"));
            ous.writeObject(sf);
            ous.flush();
            ous.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileListPath;
    }

    public synchronized void setLastQueriedFileList(FileList fileList) {
        lastQueriedFileList = fileList;
        System.out.println("Files:");
        for (String file : fileList.getFileList())
            System.out.println("   " + file);
    }

    public void disconnect() {
        PeerManager.removePeer(localAddress);
        dhtServer.stopServer();
        stabiliser.disconnect();
    }

    public String executeQuery(String input) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(baos);

        try {
            if (input.equals("nb")) {
                getNeighbourState().print(printStream, "");
            } else if (!cscOnly && input.equals("fingers")) {
                getFingerState().print(printStream, "");
            } else if (!cscOnly && input.equals("files")) {
                getDhtStore().print(printStream, "");
            } else if (!cscOnly && input.equals("stabilise")) {
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
            } else if (cscOnly && input.startsWith("files")) {
                input = input.substring("files ".length());
                printStream.println("getting file list for user " + input);
                getFileList(input, "./keys/" + input + "-public.key");
            } else if (cscOnly && input.startsWith("dl")) {
                input = input.substring("dl ".length());
                printStream.println("downloading " + input);
                getFile(input);
            } else if (cscOnly && input.startsWith("ul")) {
                input = input.substring("ul ".length());
                publishFile(input);
            } else if (!cscOnly && input.contains(" ")) {
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
