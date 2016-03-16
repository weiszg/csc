package uk.ac.cam.gw361.csc.dht;

import uk.ac.cam.gw361.csc.analysis.StateReport;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.storage.SignedFile;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.HashMap;
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

    @Override
    public DoubleAddress nextHop(DhtPeerAddress source, BigInteger target) throws IOException {
        DoubleAddress result = comm.nextHop(source, target);
        return new DoubleAddress(new DhtPeerAddress(result.neighbour),
                ((result.finger==null) ? null : new DhtPeerAddress(result.finger)));
    }

    @Override
    public NeighbourState getNeighbourState(DhtPeerAddress source) throws IOException {
        NeighbourState result = comm.getNeighbourState(source);
        return new NeighbourState(result);
    }

    @Override
    public TransferReply upload(DhtPeerAddress source, Integer port, BigInteger file)
            throws IOException {
        return comm.upload(source, port, file);
    }

    @Override
    public TransferReply download(DhtPeerAddress source, Integer port, DhtFile file)
            throws IOException {
        return comm.download(source, port, cloneDhtFile(file));
    }

    @Override
    public Boolean isAlive(DhtPeerAddress source) throws IOException {
        return comm.isAlive(source);
    }

    @Override
    public Boolean checkUserID(DhtPeerAddress source, BigInteger userID) throws IOException {
        return comm.checkUserID(source, userID);
    }

    @Override
    public Map<BigInteger, Boolean> storingFiles(DhtPeerAddress source, List<DhtFile> files)
            throws IOException {
        List<DhtFile> clonedFiles = new LinkedList<>();
        for (DhtFile file : files)
            clonedFiles.add(cloneDhtFile(file));

        return comm.storingFiles(source, clonedFiles);
    }

    @Override
    public String query(String input) throws RemoteException {
        return comm.query(input);
    }

    @Override
    public StateReport getStateReport() throws RemoteException {
        return comm.getStateReport();
    }
}
