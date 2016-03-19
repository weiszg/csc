package uk.ac.cam.gw361.csc.dht;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.ac.cam.gw361.csc.DhtHandoffTest;
import uk.ac.cam.gw361.csc.Server;
import uk.ac.cam.gw361.csc.transfer.DirectTransfer;
import uk.ac.cam.gw361.csc.transfer.FileListDownloadContinuation;
import uk.ac.cam.gw361.csc.transfer.FileUploadContinuation;
import uk.ac.cam.gw361.csc.transfer.TransferTask;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;

/**
 * Created by gellert on 18/03/2016.
 */
public class LocalPeerTest {
    static LocalPeer serverPeer;
    static LocalPeer clientPeer;
    static LocalPeer otherPeer;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    public static void setUpKeys() {
        Server.setKeyStore();
        Server.setTrustStore();
        TransferTask.waitRetry = 10;
        //System.setProperty("javax.net.debug", "all");
    }

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        setUpKeys();

        File dir = new File("./storage");
        DhtHandoffTest.deleteFile(dir);
        File file1 = new File("./keys/client-public.key");
        File file2 = new File("./keys/client-private.key");
        DhtHandoffTest.deleteFile(file1);
        DhtHandoffTest.deleteFile(file2);


        serverPeer = PeerManager.spawnPeer("node1:12001", 100000000);
        otherPeer = PeerManager.spawnPeer("node2:12002", 100000000);
        clientPeer = PeerManager.spawnPeer("client:12003", 100000000, true);

        serverPeer.join(otherPeer.localAddress.getConnectAddress());
        clientPeer.join(serverPeer.localAddress.getConnectAddress());
    }

    @Test
    public void testAddress() {
        Assert.assertTrue(serverPeer.localAddress.getPort().equals(12001));
        BigInteger expectedAddress = new BigInteger(
                "-24391412709227097335922361918582580020911004142782017157455971402604531959414");
        BigInteger realAddress = serverPeer.localAddress.getUserID();
        Assert.assertTrue(realAddress.equals(expectedAddress));
    }

    @Test
    public void testGetFileList() throws IOException {
        exception.expect(IOException.class);
        try {
            DirectTransfer ft = clientPeer.getFileList("nonexistent", "./keys/client-public.key");
        } catch (IOException e) {
            Assert.assertTrue(e.toString().startsWith("java.io.IOException: File not found"));
            throw e;
        }
    }

    @Test
    public void testPublishFile() throws IOException {
        String text = "test file";
        String filename = "test.txt";
        PrintWriter writer = new PrintWriter(filename, "UTF-8");
        writer.println(text);
        writer.close();

        DirectTransfer ft = clientPeer.publishFile("test.txt");
        Assert.assertTrue(ft.isAlive());
        Assert.assertTrue(ft.continuation instanceof FileUploadContinuation);
        ft.stopTransfer(true);
        ((FileUploadContinuation) ft.continuation).notifyFinished(ft);

        (new File("test.txt")).delete();

        try { Thread.sleep(500); } catch (InterruptedException e) { }
        // signed file list should be uploaded
        ft = clientPeer.getFileList("client", "./keys/client-public.key");
        Assert.assertTrue(ft.isAlive());
        Assert.assertTrue(ft.continuation instanceof FileListDownloadContinuation);
    }

}