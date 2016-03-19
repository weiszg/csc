package uk.ac.cam.gw361.csc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.cam.gw361.csc.dht.LocalPeerTest;
import uk.ac.cam.gw361.csc.storage.DhtFile;
import uk.ac.cam.gw361.csc.dht.LocalPeer;
import uk.ac.cam.gw361.csc.dht.NeighbourState;
import uk.ac.cam.gw361.csc.dht.PeerManager;

import java.io.*;
import java.util.*;

/**
 * Created by gellert on 11/12/2015.
 */
public class DhtHandoffTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        LocalPeerTest.setUpKeys();
    }

    @Test
    public void testHandoff() {
        NeighbourState.k = 3;
        int peerCount = 10;
        int startPort = 11000;

        System.setProperty("sun.rmi.transport.proxy.connectTimeout", "100");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "100");
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "100");

        // clean up
        File dir = new File("./storage");
        deleteFile(dir);
        createFiles(peerCount);

        List<LocalPeer> peers = new ArrayList<>();

        for (int i = 0; i < peerCount; i++) {
            LocalPeer newPeer = PeerManager.spawnPeer(Integer.toString(i) + ":" +
                    Integer.toString(startPort + i), 1000);
            if (i>0) newPeer.join("localhost:" + startPort);
            try {
                newPeer.publishEntity("test" + i + ".txt");
            } catch (IOException e) {
                Assert.assertTrue(false);
            }
            peers.add(newPeer);
        }

        System.out.println("started peers");
        try { Thread.sleep(1000); } catch (InterruptedException e) { }

        for (int i = 1; i < peerCount; i++) {
            System.out.println("stopping" + i);
            peers.get(i).disconnect();
            try { Thread.sleep(1000); } catch (InterruptedException e) { }
        }

        try { Thread.sleep(2000); } catch (InterruptedException e) { }
        LocalPeer remainingPeer = peers.get(0);
        remainingPeer.stabilise();
        remainingPeer.getDhtStore().print(System.out, "");
        List<DhtFile> responsible = remainingPeer.getDhtStore()
                .getResponsibilitiesFor(remainingPeer.localAddress);

        Assert.assertTrue(responsible.size() == 10);
    }

    private void createFiles(int numFiles) {
        try {
            for (Integer i = 0; i < numFiles; i++) {
                String text = "test file " + i;
                String filename = "test" + i + ".txt";
                PrintWriter writer = new PrintWriter(filename, "UTF-8");
                writer.println(text);
                writer.close();
            }
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            Assert.assertTrue(false);
        }
    }

    private void deleteCreatedFiles(int numFiles) {
        for (Integer i = 0; i < numFiles; i++) {
            deleteFile(new File("test " + i + ".txt"));
        }
    }

    public static void deleteFile(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                deleteFile(c);
        }
        if (!f.delete())
            Assert.assertTrue(false);
    }

}
