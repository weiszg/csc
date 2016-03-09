package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.storage.DhtFile;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

/**
 * Created by gellert on 09/03/2016.
 */
public class CscServer implements CscComm {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private final int port;
    private final Registry registry;
    private final DhtServer server;

    public CscServer(LocalPeer localPeer, int port, Registry registry, DhtServer server) {
        this.port = port;
        this.localPeer = localPeer;
        this.registry = registry;
        this.server = server;
    }

    // publicly exposed CscComm part

    @Override
    public DhtPeerAddress lookup(BigInteger target) throws IOException {
        return localPeer.getClient().lookup(target);
    }

    @Override
    public Long upload(String host, Integer port, BigInteger file) throws IOException {
        // the DhtPeerAddress only matters for InternalUploadContinuations
        // which doesn't go through here
        return server.doUpload(new DhtPeerAddress(BigInteger.ZERO, host, port,
                        localPeer.localAddress.getUserID()),
                port, file);
    }

    @Override
    public Integer download(String host, Integer port, DhtFile file) throws IOException {
        return server.doDownload(new DhtPeerAddress(BigInteger.ZERO, host, port,
                        localPeer.localAddress.getUserID()),
                port, file);
    }

    @Override
    public Boolean isAlive() throws RemoteException {
        return true;
    }
}
