package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.analysis.HopCountReporter;
import uk.ac.cam.gw361.csc.analysis.Profiler;
import uk.ac.cam.gw361.csc.analysis.Reporter;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.storage.Hasher;
import uk.ac.cam.gw361.csc.storage.SignedFile;
import uk.ac.cam.gw361.csc.transfer.DirectTransfer;
import uk.ac.cam.gw361.csc.transfer.FileUploadContinuation;
import uk.ac.cam.gw361.csc.transfer.TransferContinuation;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by gellert on 01/11/2015.
 */
public class DhtClient {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private final boolean debugClient = false;
    private Map<DhtPeerAddress, Remote> connections = new HashMap<>();
    private Map<DhtPeerAddress, Long> lastUsed = new HashMap<>();
    private long cacheTime = 10000;  // how long to cache connections
    private Reporter connectReporter, lookupReporter;
    final SSLContext secureContext;
    private final ServerSocketFactory sslServerFactory;
    final SocketFactory sslFactory;
    public boolean disableCaching = false;

    public DhtClient(LocalPeer localPeer) throws NoSuchAlgorithmException, NoSuchProviderException,
            KeyManagementException{
        if (PeerManager.perfmon)
            connectReporter = new Reporter("connectLatency.csv");
        if (PeerManager.perfmon)
            lookupReporter = new Reporter("lookupLatency.csv");

        secureContext = SSLContext.getDefault();
        // secureContext.init(null, null, null);  // default initialised automatically
        sslFactory = secureContext.getSocketFactory();
        sslServerFactory = secureContext.getServerSocketFactory();

        this.localPeer = localPeer;
    }

    public void bootstrap(String host)
            throws IOException {
        int port = 8000;
        if (host.contains(":")) {
            port = Integer.parseInt(host.split(":")[1]);
            host = host.split(":")[0];
        }
        DhtPeerAddress pred = doLookup(new DhtPeerAddress(null, host, port,
                        localPeer.localAddress.getUserID()),
                localPeer.localAddress.getUserID(), false, null);
        localPeer.getNeighbourState().addNeighbour(pred);
    }

    private ServerSocket createServerSocket(int port) throws IOException {
        boolean useSSL = localPeer.isCscOnly();
        if (useSSL)
            return sslServerFactory.createServerSocket(port);
        else
            return new ServerSocket(port);
    }

    private Socket createSocket() throws IOException {
        boolean useSSL = localPeer.isCscOnly();
        if (useSSL)
            return sslFactory.createSocket();
        else
            return new Socket();
    }

    private Remote connect(DoubleAddress server) throws ConnectionFailedException {
        if (server.finger != null) {
            server.fingerAlive = true;
            try {
                Remote comm = connect(server.finger);
                if (comm != null)
                    return comm;
            } catch (ConnectionFailedException e) {
                // we'll return the neighbour connect instead
            }
        }

        server.fingerAlive = false;
        return connect(server.neighbour);
    }

    private Remote connect(DhtPeerAddress server) throws ConnectionFailedException {
        // don't connect to myself through RMI
        if (!localPeer.isCscOnly() && server.equals(localPeer.localAddress))
            return new LocalDhtCommWrapper(localPeer.getServer());

        if (!localPeer.isCscOnly() && PeerManager.allowLocalConnect) {
            if (server.getUserID() != null && PeerManager.hasPeer(server))
                return PeerManager.getServer(server);
            if (server.getHost() != null && server.getHost().equals("localhost") &&
                    PeerManager.hasPeer(server.getPort()))
                 return PeerManager.getServer(server.getPort());
        }

        Profiler profiler = null;
        if (PeerManager.perfmon)
            profiler = new Profiler(connectReporter);

        Remote comm;
        try {
            comm = doConnect(server);
            return comm;
        } finally {
            if (PeerManager.perfmon) profiler.end();
        }
    }

    private Remote doConnect(DhtPeerAddress server) throws ConnectionFailedException {
        if (debug) server.print(System.out, "client connect: ");

        Profiler profiler = null;
        if (debugClient && localPeer.isCscOnly()) profiler = new Profiler(new Reporter(System.out));

        // cache lookup has to be synchronised
        Remote cached = null;
        if (!disableCaching) {
            synchronized (connections) {
                if (server.getUserID() != null && connections.containsKey(server)) {
                    cached = connections.get(server);
                    lastUsed.put(server, System.currentTimeMillis());
                }
            }
        }

        try {
            if (cached != null) {
                try {
                    if (!localPeer.isCscOnly()) {
                        if (server.getUserID() != null)
                            if (!(cached instanceof DhtComm) || !((DhtComm) cached).checkUserID(localPeer.localAddress, server.getUserID()))
                                throw new RemoteException();
                            else if (!((DhtComm) cached).isAlive(localPeer.localAddress))
                                throw new RemoteException();
                    } else {
                        if (!(cached instanceof CscComm) || !((CscComm) cached).isAlive())
                            throw new RemoteException();
                    }
                    return cached;
                } catch (IOException e) {
                    connections.remove(server);
                    lastUsed.remove(server);
                }
            }

            try {
                Registry registry = LocateRegistry.getRegistry(server.getHost(), server.getPort());
                Remote ret;
                if (localPeer.isCscOnly())
                    ret = registry.lookup("CscComm");
                else {
                    ret = registry.lookup("DhtComm");
                    if (server.getUserID() != null && !localPeer.isCscOnly()
                            && !((DhtComm) ret).checkUserID(localPeer.localAddress, server.getUserID()))
                        throw new ConnectionFailedException("UserID mismatch", server);
                }

                // cache the connection
                synchronized (connections) {
                    if (server.getUserID() != null) {
                        connections.put(server, ret);
                        lastUsed.put(server, System.currentTimeMillis());
                    }
                }
                return ret;
            } catch (IOException | NotBoundException e) {
                if (debug) System.err.println("Client exception: " + e.toString());
                throw new ConnectionFailedException(e.toString(), server);
            }
        } finally {
            if (profiler != null)
                profiler.end();
        }
    }

    public DhtPeerAddress lookup(final BigInteger target) throws IOException {
        return lookup(target, false, null);
    }

    public DhtPeerAddress lookup(final BigInteger target, boolean trace, HopCountReporter reporter)
            throws IOException {
        // navigate to the highest peer lower than the target
        Profiler profiler = null;
        if (PeerManager.perfmon)
            profiler = new Profiler(lookupReporter);

        try {
            if (debug) System.out.println("client lookup");
            if (trace) System.out.println("lookup to " + target.toString());
            DoubleAddress start = localPeer.getNextLocalHop(target);
            if (trace)
                start.print(System.out, "start: ");

            return doLookup(start, target, trace, reporter);
        } finally {
            if (PeerManager.perfmon)
                profiler.end();
        }
    }

    private DhtPeerAddress doLookup(DhtPeerAddress start, BigInteger target,
                                    boolean trace, HopCountReporter reporter)
            throws IOException {
        return doLookup(new DoubleAddress(start, null), target, trace, reporter);
    }

    private DhtPeerAddress doLookup(DoubleAddress start, BigInteger target,
                                    boolean trace, HopCountReporter reporter)
            throws IOException {
        if (debug) System.out.println("client dolookup");
        if (localPeer.isCscOnly()) {
            CscComm cscomm = ((CscComm) connect(start));
            DhtPeerAddress result = cscomm.lookup(target);

            // fix relatives and host names
            DhtPeerAddress prevAddress = start.fingerAlive ? start.finger : start.neighbour;
            result.setRelative(localPeer.localAddress.getUserID());
            if (result.getHost().equals("localhost"))
                result.setHost(prevAddress.getHost());

            return result;
        }

        DoubleAddress nextHop = start, prevHop = null;
        int hop = 0;
        int fingerUsed = 0;

        do {
            hop++;
            prevHop = nextHop;
            DhtComm comm = ((DhtComm) connect(prevHop));
            nextHop = comm.nextHop(localPeer.localAddress, target);

            if (prevHop.fingerAlive)
                fingerUsed++;

            // fix relatives and host names
            DhtPeerAddress prevAddress = prevHop.fingerAlive ? prevHop.finger : prevHop.neighbour;
            nextHop.neighbour.setRelative(localPeer.localAddress.getUserID());
            if (nextHop.neighbour.getHost().equals("localhost"))
                nextHop.neighbour.setHost(prevAddress.getHost());

            if (nextHop.finger != null) {
                nextHop.finger.setRelative(localPeer.localAddress.getUserID());
                if (nextHop.finger.getHost().equals("localhost"))
                    nextHop.finger.setHost(prevAddress.getHost());
            }

            if (trace) nextHop.print(System.out, "hop " + hop + ": ");
        } while (!nextHop.neighbour.equals(prevHop.neighbour));

        hop--;
        if (reporter != null)
            reporter.report(start.neighbour.getUserID(), target, nextHop.neighbour.getUserID(),
                    hop, fingerUsed);

        return nextHop.neighbour;
    }

    public NeighbourState getNeighbourState(DhtPeerAddress peer)
            throws IOException {
        if (debug) System.out.println("client getneighbourstate");
        NeighbourState result;
        DhtComm comm = ((DhtComm) connect(peer));
        try {
            result = comm.getNeighbourState(localPeer.localAddress);
            if (result == null)
                throw new PeerNotStableException();

            result.setLocalAddress(localPeer.localAddress);
            return result;
        } catch (RemoteException e) {
            if (debug) e.printStackTrace();
            throw e;
        }
    }

    public Map<BigInteger, Boolean> storingFiles(DhtPeerAddress peer, List<DhtFile> files)
            throws IOException {
        if (debug) System.out.println("client getstoringfiles");
        Map<BigInteger, Boolean> result;
        DhtComm comm = ((DhtComm) connect(peer));
        try {
            result = comm.storingFiles(localPeer.localAddress, files);
            return result;
        } catch (RemoteException e) {
            if (debug) e.printStackTrace();
            throw e;
        }
    }

    public DirectTransfer download(String fileName, DhtFile file,
                                TransferContinuation continuation) throws IOException {
        DirectTransfer ft = null;
        DhtPeerAddress target = lookup(file.hash);
        if (file.hash != null)
            ft = doDownload(target, fileName, file, continuation);
        // track transfer with ft
        return ft;
    }

    private DirectTransfer doDownload(DhtPeerAddress peer, String fileName, DhtFile file,
                                      TransferContinuation continuation) throws IOException {
        if (debug) System.out.println("client download");

        DhtComm comm=null; CscComm csccomm=null;
        if (localPeer.isCscOnly())
            csccomm = ((CscComm) connect(peer));
        else
            comm = ((DhtComm) connect(peer));

        DirectTransfer ft = null;
        try {
            TransferReply reply;
            if (localPeer.isCscOnly()) {
                ft = new DirectTransfer(localPeer, peer, ((Socket) null), fileName, true,
                        file, continuation, localPeer.isCscOnly());
                reply = csccomm.upload(localPeer.localAddress.getHost(), file.hash);
            } else {
                ServerSocket listener = createServerSocket(0);
                // the owner doesn't matter, the destination of the download could be a different folder
                ft = new DirectTransfer(localPeer, peer, listener, fileName, true,
                        file, continuation, localPeer.isCscOnly());
                ft.start();
                reply = comm.upload(localPeer.localAddress, listener.getLocalPort(), file.hash);
            }

            if (reply.primary == null)
                throw new IOException("Size null");
            else if (localPeer.isCscOnly()) {  // client mode
                Socket socket = createSocket();
                socket.setSoTimeout(60000);
                socket.connect(new InetSocketAddress(peer.getHost(), reply.port), 3000);
                ft = new DirectTransfer(localPeer, peer, socket, fileName, true,
                        file, continuation, localPeer.isCscOnly());
                ft.start();
            }
        } catch (IOException ioe) {
            if (ft != null)
                ft.stopTransfer(false, false);
            if (debug) ioe.printStackTrace();
            throw ioe;
        }
        return ft;
    }

    public DirectTransfer upload(DhtPeerAddress target, BigInteger file,
                              TransferContinuation continuation) throws IOException {
        if (!localPeer.getDhtStore().containsFile(file))
            throw new IOException("File not found for upload: " + file.toString() + " at " +
            localPeer.localAddress.getConnectAddress());
        String fileName = localPeer.getDhtStore().getFolder() + "/" + file.toString();

        DhtFile dbFile = localPeer.getDhtStore().getFile(file);
        DirectTransfer ft = doUpload(target, dbFile, fileName, continuation);
        // track transfer with ft
        return ft;
    }

    public DirectTransfer upload(String name, FileUploadContinuation continuation)
            throws IOException {
        BigInteger fileHash = Hasher.hashFile(name);
        File file = new File(name);
        if (!file.exists())
            throw new IOException("File not found for upload");

        DirectTransfer ft = null;
        DhtPeerAddress target = lookup(fileHash);
        if (fileHash != null) {
            // owner is target
            DhtFile uploadFile = new DhtFile(fileHash, file.length(), target);
            ft = doUpload(target, uploadFile, name, continuation);
        }
        // track transfer with ft
        return ft;
    }

    public DirectTransfer signedUpload(String name, BigInteger fileID, long timestamp,
            FileUploadContinuation continuation) throws IOException {
        // used for uploads signed with a private key - they aren't hash checked and can be uploaded
        // to an arbitrary target
        File file = new File(name + ".signed");
        if (!file.exists())
            throw new IOException("File not found for signedUpload");

        DirectTransfer ft = null;
        DhtPeerAddress target = lookup(fileID);
        if (fileID != null) {
            // owner is target
            System.out.println("Signed hash is " + fileID.toString() +
                    ", corresponding timestamp is " + timestamp);
            DhtFile uploadFile = new SignedFile(fileID, file.length(), target, timestamp);
            ft = doUpload(target, uploadFile, name, continuation);
        }

        return ft;
    }

    private DirectTransfer doUpload(DhtPeerAddress peer, DhtFile file, String fileName,
                                 TransferContinuation continuation) throws IOException {
        if (debug) System.out.println("client upload");

        DhtComm comm=null; CscComm csccomm=null;
        if (localPeer.isCscOnly())
            csccomm = ((CscComm) connect(peer));
        else
            comm = ((DhtComm) connect(peer));

        DirectTransfer ft = null;
        try {
            TransferReply response;

            if (localPeer.isCscOnly()) {
                ft = new DirectTransfer(localPeer, peer, ((Socket) null), fileName,
                        false, file, continuation, localPeer.isCscOnly());
                response = csccomm.download(localPeer.localAddress.getHost(), file);
            } else {
                ServerSocket listener = createServerSocket(0);
                ft = new DirectTransfer(localPeer, peer, listener, fileName,
                        false, file, continuation, localPeer.isCscOnly());
                ft.start();

                response = comm.download(localPeer.localAddress, listener.getLocalPort(), file);
            }

            if (response.primary == 1) {
                System.out.println("Out of range for receiver " + peer.getConnectAddress());
                ft.stopTransfer(false);
            } else if (response.primary == 2) {
                System.out.println("redundant: " + file.hash.toString());
                ft.stopTransfer(true);
            } else { // else the transfer is on
                if (localPeer.isCscOnly()) {  // we're in client mode, start transfer
                    Socket socket = createSocket();
                    socket.setSoTimeout(60000);
                    socket.connect(new InetSocketAddress(
                            peer.getHost(), response.port), 3000);
                    ft = new DirectTransfer(localPeer, peer, socket, fileName, false,
                            file, continuation, localPeer.isCscOnly());
                    ft.start();
                }
            }
        } catch (IOException ioe) {
            if (debug) ioe.printStackTrace();
            throw ioe;
        }
        return ft;
    }

    void vacuumConnectionCache() {
        LinkedList<DhtPeerAddress> evict = new LinkedList<>();
        synchronized (connections) {
            for (Map.Entry<DhtPeerAddress, Long> e : lastUsed.entrySet()) {
                if (e.getValue() < System.currentTimeMillis() - cacheTime) {
                    // cache entry is old and has to be evicted
                    evict.add(e.getKey());
                }
            }

            for (DhtPeerAddress a : evict) {
                connections.remove(a);
                lastUsed.remove(a);
            }
        }
    }

    public String query(DhtPeerAddress peer, String input) throws IOException {
        if (debug) System.out.println("client query");
        DhtComm comm = ((DhtComm) connect(peer));
        return comm.query(input);
    }
}

class ConnectionFailedException extends IOException {
    String reason;
    DhtPeerAddress connectTo;
    ConnectionFailedException() {}
    ConnectionFailedException(String reason, DhtPeerAddress connectTo) {
        this.reason = reason;
        this.connectTo = connectTo;
    }

    @Override
    public String toString() {
        return "ConnectionFailedException: " + reason +
                ", target: " + connectTo.getConnectAddress();
    }
}

class PeerNotStableException extends RemoteException {
}

