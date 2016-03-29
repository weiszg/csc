package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.analysis.StateReport;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.transfer.DirectTransfer;
import uk.ac.cam.gw361.csc.transfer.InternalDownloadContinuation;
import uk.ac.cam.gw361.csc.transfer.InternalUploadContinuation;
import uk.ac.cam.gw361.csc.transfer.TransferContinuation;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;
import java.util.HashSet;
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
    private final ServerSocketFactory sslServerFactory = SSLServerSocketFactory.getDefault();
    private final SocketFactory sslFactory = SSLSocketFactory.getDefault();

    public DhtServer(LocalPeer localPeer, int port)
            throws NoSuchAlgorithmException, NoSuchProviderException, KeyManagementException {
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
                // SSLContext secureContext = SSLContext.getDefault();
                // secureContext.init(null, null, null);  // default context initialised automatically
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

    private boolean allowedConnection(String host) {
        return true;  // todo: implement DMZ policy
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
                if (!allowedConnection(clientHost))
                    throw new ServerNotActiveException();

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

    private ServerSocket createServerSocket(int port, boolean useSSL) throws IOException {
        if (useSSL)
            return sslServerFactory.createServerSocket(port);
        else
            return new ServerSocket(port);
    }

    private Socket createSocket(boolean useSSL) throws IOException {
        if (useSSL)
            return sslFactory.createSocket();
        else
            return new Socket();
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

    TransferReply doUpload(DhtPeerAddress source, Integer port, BigInteger file, boolean useSSL)
            throws IOException {
        boolean clientMode = (port == null);
        DhtFile transferFile = localPeer.getDhtStore().getFile(file);
        String fileName = localPeer.getDhtStore().getFolder() + "/" + file.toString();

        Thread uploader;
        // no continuation for cscOnly transfers
        TransferContinuation continuation = useSSL ? null : new InternalDownloadContinuation();

        if (clientMode) {
            ServerSocket listener = createServerSocket(0, useSSL);
            uploader = new DirectTransfer(localPeer, source, listener, fileName, false,
                    transferFile, continuation, useSSL);
            uploader.start();
            return new TransferReply(transferFile.size, listener.getLocalPort());
        } else {
            Socket socket = createSocket(useSSL);
            socket.setSoTimeout(60000);
            socket.connect(new InetSocketAddress(source.getHost(), port), 3000);

            uploader = new DirectTransfer(localPeer, source, socket, fileName, false,
                    transferFile, continuation, useSSL);
            uploader.start();
            return new TransferReply(transferFile.size, null);
        }
    }

    @Override
    public TransferReply upload(DhtPeerAddress source, Integer port, BigInteger file)
            throws IOException {
        acceptConnection(source);
        return doUpload(source, port, file, false);
    }

    @Override
    public TransferReply download(DhtPeerAddress source, Integer port, DhtFile file)
            throws IOException {
        // return value: 0 for ACCEPT, 1 for DECLINE and 2 for REDUNDANT
        acceptConnection(source);
        return doDownload(source, port, file, false);
    }

    TransferReply doDownload(DhtPeerAddress source, Integer port, DhtFile file, boolean useSSL)
            throws IOException {
        boolean clientMode = (port == null);
        file.owner.setRelative(localPeer.localAddress.getUserID());
        
        // do not accept if I already have the file
        // or if a download with the same hash is in progress
        if (localPeer.getDhtStore().hasFile(file) ||
                localPeer.getTransferManager().hasExclusiveHash(file.hash))
            return new TransferReply(2, null);

        System.out.println("Storing file at " + localPeer.userName + "/" +
                file.hash.toString());

        String fileName = localPeer.getDhtStore().getFolder() + "/" + file.hash;

        if (clientMode) {
            ServerSocket listener = createServerSocket(0, useSSL);
            Thread downloader = new DirectTransfer(localPeer, source, listener, fileName,
                    true, file, new InternalDownloadContinuation(), useSSL);
            downloader.start();
            return new TransferReply(0, listener.getLocalPort());
        } else {
            Socket socket = createSocket(useSSL);
            socket.setSoTimeout(60000);
            socket.connect(new InetSocketAddress(source.getHost(), port), 3000);
            Thread downloader = new DirectTransfer(localPeer, source, socket, fileName,
                    true, file, new InternalDownloadContinuation(), useSSL);
            downloader.start();
            return new TransferReply(0, null);
        }
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
        long time = System.currentTimeMillis();
        long upTimestamp = DirectTransfer.externalLimiter.lastSendTimestamp;
        long downTimestamp = DirectTransfer.externalLimiter.lastRecTimestamp;
        long upBytes = (upTimestamp < time - 1000) ? 0 : DirectTransfer.externalLimiter.bytesSent;
        long downBytes = (downTimestamp < time - 1000) ? 0 : DirectTransfer.externalLimiter.bytesReceived;

        StateReport report = new StateReport(
                new HashSet<>(localPeer.getDhtStore().getStoredFiles().values()),
                replication,
                localPeer.getNeighbourState().getPredecessors().size(),
                localPeer.getNeighbourState().getSuccessors().size(),
                localPeer.getStabiliser().millisSinceStabilised(),
                (float)upBytes / (50 + time - upTimestamp),
                (float)downBytes / (50 + time - downTimestamp));
        return report;
    }
}
