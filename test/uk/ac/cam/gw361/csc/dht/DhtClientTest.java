package uk.ac.cam.gw361.csc.dht;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.ac.cam.gw361.csc.Server;
import uk.ac.cam.gw361.csc.dht.DhtPeerAddress;
import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.dht.PeerManager;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.transfer.DirectTransfer;
import uk.ac.cam.gw361.csc.transfer.FileDownloadContinuation;
import uk.ac.cam.gw361.csc.transfer.TransferTask;

import java.io.IOException;
import java.math.BigInteger;
import java.util.TreeSet;

/**
 * Created by gellert on 18/03/2016.
 */
public class DhtClientTest {
    static LocalPeer node1, node2;
    static final boolean debug = false;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        LocalPeerTest.setUpKeys();

        node1 = PeerManager.spawnPeer("node1:9001", 100000000);
        node2 = PeerManager.spawnPeer("node2:9002", 100000000);
        node1.getClient().bootstrap("127.0.0.1:9002");
    }

    @Test
    public void testBootstrap() throws IOException {
        if (debug) node1.getNeighbourState().print(System.out, "node1 neighbours: ");
        TreeSet<DhtPeerAddress> neighbours = node1.getNeighbourState().getNeighbours();

        Assert.assertTrue(neighbours.size() == 1);
        Assert.assertTrue(neighbours.first().equals(node2.localAddress));
        Assert.assertTrue(neighbours.first().getPort().equals(node2.localAddress.getPort()));
    }

    @Test
    public void testLookup() throws IOException{
        boolean traceHops = false;

        BigInteger address1 = node1.localAddress.getUserID();
        BigInteger address2 = node2.localAddress.getUserID();

        node1.localAddress.print(System.out, "node1 address: ");
        node2.localAddress.print(System.out, "node2 address: ");

        // should be node2
        DhtPeerAddress result1 = node1.getClient().lookup(address1.subtract(BigInteger.ONE), traceHops, null);
        if (debug) result1.print(System.out, "lookup result: ");
        Assert.assertTrue(result1.equals(node2.localAddress));

        // should be node2
        DhtPeerAddress result2 = node1.getClient().lookup(address1, traceHops, null);
        if (debug) result2.print(System.out, "lookup result: ");
        Assert.assertTrue(result2.equals(node2.localAddress));

        // should be node1
        DhtPeerAddress result3 = node1.getClient().lookup(address1.add(BigInteger.ONE), traceHops, null);
        if (debug) result3.print(System.out, "lookup result: ");
        Assert.assertTrue(result3.equals(node1.localAddress));

        // should be node1
        DhtPeerAddress result4 = node1.getClient().lookup(address2.subtract(BigInteger.ONE), traceHops, null);
        if (debug) result4.print(System.out, "lookup result: ");
        Assert.assertTrue(result4.equals(node1.localAddress));

        // should be node1
        DhtPeerAddress result5 = node1.getClient().lookup(address2, traceHops, null);
        if (debug) result5.print(System.out, "lookup result: ");
        Assert.assertTrue(result5.equals(node1.localAddress));

        // should be node2
        DhtPeerAddress result6 = node1.getClient().lookup(address2.add(BigInteger.ONE), traceHops, null);
        if (debug) result6.print(System.out, "lookup result: ");
        Assert.assertTrue(result6.equals(node2.localAddress));
    }

    @Test
    public void testGetNeighbourState() throws IOException {
        NeighbourState n2 = node1.getClient().getNeighbourState(node2.localAddress);
        if (true) n2.print(System.out, "");

        TreeSet<DhtPeerAddress> neighbours = n2.getNeighbours();
        Assert.assertTrue(neighbours.size() == 1);
        Assert.assertTrue(neighbours.first().equals(node1.localAddress));
        Assert.assertTrue(neighbours.first().getPort().equals(node1.localAddress.getPort()));
    }

    @Test
    public void testDownload() throws IOException {
        exception.expect(IOException.class);
        DirectTransfer ft = null;
        try {
            ft = node1.getClient().download("nonexistent",
                    new DhtFile(BigInteger.ZERO, null, null), null);
        } catch (IOException e) {
            Assert.assertTrue(e.toString().equals("java.io.IOException: File not found: 0"));
            throw e;
        }
    }
}
