package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.analysis.StateReport;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.transfer.DirectTransfer;
import uk.ac.cam.gw361.csc.transfer.InternalDownloadContinuation;
import uk.ac.cam.gw361.csc.transfer.InternalUploadContinuation;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by gellert on 01/11/2015.
 */
public class DhtServer implements DhtComm {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private Map<DhtPeerAddress, Socket> uploads = new HashMap<>();
    private final int port;
    private final Registry registry;
    private CscServer cscServer;
    private final boolean localOnly;

    public DhtServer(LocalPeer localPeer, int port) {
        this.localOnly = false;
        this.port = port;
        this.localPeer = localPeer;
        Registry reg = null;
        try {
            try {
                reg = LocateRegistry.createRegistry(port);
            } catch (ExportException e) {
                reg = LocateRegistry.getRegistry(port);
            }
        } catch (RemoteException e) {
            System.err.println("DHT Server exception: " + e.toString());
            e.printStackTrace();
        } finally {
            registry = reg;
            try {
                cscServer = new CscServer(localPeer, this);
            } catch (IOException e) {
                System.err.println("Client-facing server failed to start: " + e.toString());
            }
        }
    }

    public DhtServer(DhtServer toCopy, boolean local) {
        this.localPeer = toCopy.localPeer;
        this.uploads = toCopy.uploads;
        this.port = toCopy.port;
        this.registry = toCopy.registry;
        this.cscServer = toCopy.cscServer;
        this.localOnly = local;
    }

    public void startServer() {
        try {
            DhtComm dhtstub = (DhtComm) UnicastRemoteObject.exportObject(this, 0);
            // bind the remote object's stub in the registry
            try {
                registry.bind("DhtComm", dhtstub);
            } catch (AlreadyBoundException e) {
                registry.rebind("DhtComm", dhtstub);
            }

            CscComm cscstub = cscServer;
            try {
                registry.bind("CscComm", cscstub);
            } catch (AlreadyBoundException e) {
                registry.rebind("CscComm", cscstub);
            }

            if (debug) System.out.println("DHT Server ready on " + port);
        } catch (RemoteException e) {
            System.err.println("DHT Server exception: " + e.toString());
        }
    }

    public void stopServer() {
        try {
            registry.unbind("DhtComm");
            registry.unbind("CscComm");
            UnicastRemoteObject.unexportObject(this, true);
            UnicastRemoteObject.unexportObject(cscServer, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // private-ring DhtComm part

    private String getClientHost() throws ServerNotActiveException {
        if (localOnly)
            return localPeer.localAddress.getHost();
        else
            return RemoteServer.getClientHost();
    }

    private void acceptConnection(DhtPeerAddress source) throws IOException {
        try {
            if (PeerManager.allowLocalConnect && PeerManager.hasPeer(source)) {
                // locally connected
                String host = InetAddress.getLocalHost().getHostAddress();
                DhtPeerAddress newSource = new DhtPeerAddress(source.getUserID(), host,
                        source.getPort(), localPeer.localAddress.getUserID());
                localPeer.getNeighbourState().addNeighbour(newSource);
            } else {
                String clientHost = getClientHost();
                // todo: check if local/trusted
                if (!clientHost.startsWith("192.168.1.") ||
                        Integer.parseInt(clientHost.substring("192.168.1.".length())) < 107) {
                    System.err.println("Denying request from " + clientHost);
                    throw new ServerNotActiveException();

                }
                source.setRelative(localPeer.localAddress.getUserID());
                // set host of source
                source.setHost(clientHost);
                localPeer.getNeighbourState().addNeighbour(source);
            }
        } catch (ServerNotActiveException | UnknownHostException e) {
            // disallow access
            e.printStackTrace();
            throw new IOException("Permission denied");
        }
    }

    @Override
    public DoubleAddress nextHop(DhtPeerAddress source, BigInteger target) throws IOException {
        if (debug) System.out.println("server lookup");
        acceptConnection(source);
        return localPeer.getNextLocalHop(target);
    }

    @Override
    public NeighbourState getNeighbourState(DhtPeerAddress source) throws IOException {
        if (debug) System.out.println("server getneighbourstate");
        acceptConnection(source);
        if (localPeer.isStable())
            return localPeer.getNeighbourState();
        else return null;
    }

    Long doUpload(DhtPeerAddress source, Integer port, BigInteger file) throws IOException {
        DhtFile transferFile = localPeer.getDhtStore().getFile(file);
        String fileName = localPeer.getDhtStore().getFolder() + "/" + file.toString();

        Socket socket = new Socket();
        socket.setSoTimeout(10000);
        socket.connect(new InetSocketAddress(source.getHost(), port), 9000);

        Thread uploader = new DirectTransfer(localPeer, source, socket, fileName, false,
                transferFile, new InternalUploadContinuation());
        uploader.start();
        return transferFile.size;
    }

    @Override
    public Long upload(DhtPeerAddress source, Integer port, BigInteger file)
            throws IOException {
        acceptConnection(source);
        return doUpload(source, port, file);
    }

    @Override
    public Integer download(DhtPeerAddress source, Integer port, DhtFile file) throws IOException {
        // return value: 0 for ACCEPT, 1 for DECLINE and 2 for REDUNDANT
        acceptConnection(source);
        return doDownload(source, port, file);
    }

    Integer doDownload(DhtPeerAddress source, Integer port, DhtFile file) throws IOException {
        file.owner.setRelative(localPeer.localAddress.getUserID());
        // only accept if owner is within predecessor range
        // or if I am between the current owner and the file, in which case I'll be the next owner
        if (!file.owner.equals(localPeer.localAddress) &&
                !localPeer.localAddress.isBetween(file.owner,
                        new DhtPeerAddress(file.hash, null, null,
                                localPeer.localAddress.getUserID())) &&
                !localPeer.getNeighbourState().getSuccessors().contains(file.owner)) {
            System.out.println("Refusing download from " + source.getConnectAddress()
                    + " with owner " + file.owner.getConnectAddress());
            return 1;
        } else if (localPeer.getDhtStore().hasFile(file))
            return 2;

        System.out.println("Storing file at " + localPeer.userName + "/" +
                file.hash.toString());

        String fileName = localPeer.getDhtStore().getFolder() + "/" + file.hash;
        Socket socket = new Socket();
        socket.setSoTimeout(10000);

        socket.connect(new InetSocketAddress(source.getHost(), port), 9000);

        Thread downloader = new DirectTransfer(localPeer, source, socket, fileName, true, file,
                new InternalDownloadContinuation());
        downloader.start();
        return 0;
    }

    @Override
    public Map<BigInteger, Boolean> storingFiles(DhtPeerAddress source, List<DhtFile> files)
            throws IOException {
        // get a list of files with their owners. Return a list of file IDs associated with
        // bools describing whether they are stored locally. Update owners in the meantime
        // if necessary
        acceptConnection(source);

        Map<BigInteger, Boolean> result = new HashMap<>();
        for (DhtFile file : files) {
            file.owner.setRelative(localPeer.localAddress.getUserID());
            boolean hasFile = localPeer.getDhtStore().hasFile(file);
            result.put(file.hash, hasFile);
            localPeer.getDhtStore().refreshResponsibility(file.hash, source, false);
        }
        return result;
    }

    @Override
    public Boolean isAlive(DhtPeerAddress source) throws IOException {
        acceptConnection(source);
        return true;
    }

    @Override
    public Boolean checkUserID(DhtPeerAddress source, BigInteger userID) throws IOException {
        acceptConnection(source);
        return userID.equals(localPeer.localAddress.getUserID());
    }

    @Override
    public String query(String input) {
        return localPeer.executeQuery(input);
    }

    @Override
    public StateReport getStateReport() {
        HashMap<BigInteger, Integer> replication = localPeer.getStabiliser().getReplicationDegree();
        StateReport report = new StateReport(replication,
                localPeer.getNeighbourState().getPredecessors().size(),
                localPeer.getNeighbourState().getSuccessors().size(),
                localPeer.getStabiliser().millisSinceStabilised());
        return report;
    }
}
