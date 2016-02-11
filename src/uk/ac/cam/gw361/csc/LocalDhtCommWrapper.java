package uk.ac.cam.gw361.csc;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by gellert on 19/01/2016.
 */
public class LocalDhtCommWrapper implements DhtComm {
    // Wraps a DhtComm element and clones results. Necessary to keep RMI semantics with
    // PeerManager.allowLocalConnect. Otherwise eg calling setRelative on the return
    // DhtPeerAddress would modify the state of the server
    private DhtComm comm;

    LocalDhtCommWrapper(DhtComm comm) {
        this.comm = comm;
    }

    private DhtFile cloneDhtFile(DhtFile file) {
        if (file instanceof SignedFile)
            return new SignedFile((SignedFile)file);
        else
            return new DhtFile(file);
    }

    public DoubleAddress nextHop(DhtPeerAddress source, BigInteger target) throws IOException {
        DoubleAddress result = comm.nextHop(source, target);
        return new DoubleAddress(new DhtPeerAddress(result.neighbour),
                ((result.finger==null) ? null : new DhtPeerAddress(result.finger)));
    }

    public NeighbourState getNeighbourState(DhtPeerAddress source) throws RemoteException {
        NeighbourState result = comm.getNeighbourState(source);
        return new NeighbourState(result);
    }

    public Long upload(DhtPeerAddress source, Integer port, BigInteger file) throws IOException {
        return comm.upload(source, port, file);
    }

    public Integer download(DhtPeerAddress source, Integer port, DhtFile file) throws IOException {
        return comm.download(source, port, cloneDhtFile(file));
    }

    public Boolean isAlive(DhtPeerAddress source) throws RemoteException {
        return comm.isAlive(source);
    }

    public Boolean checkUserID(DhtPeerAddress source, BigInteger userID) throws RemoteException {
        return comm.checkUserID(source, userID);
    }

    public Map<BigInteger, Boolean> storingFiles(DhtPeerAddress source, List<DhtFile> files)
            throws IOException {
        List<DhtFile> clonedFiles = new LinkedList<>();
        for (DhtFile file : files)
            clonedFiles.add(cloneDhtFile(file));

        return comm.storingFiles(source, clonedFiles);
    }

    public String query(String input) throws RemoteException {
        return comm.query(input);
    }
}
