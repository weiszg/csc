package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.storage.DhtFile;

import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by gellert on 09/03/2016.
 */
public class CscServer extends UnicastRemoteObject implements CscComm {
    private LocalPeer localPeer;
    private final boolean debug = false;
    private final DhtServer server;

    public CscServer(LocalPeer localPeer, DhtServer server)
            throws IOException {
        super(0, new SslRMIClientSocketFactory(), new SslRMIServerSocketFactory());

        this.localPeer = localPeer;
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
