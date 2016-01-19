package uk.ac.cam.gw361.csc;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by gellert on 02/11/2015.
 */
public class NeighbourState implements Serializable {
    public static int k = 5;
    private boolean debug = false;
    private transient DhtPeerAddress localAddress;
    private TreeSet<DhtPeerAddress> successors = new TreeSet<>();
    private TreeSet<DhtPeerAddress> predecessors = new TreeSet<>();

    NeighbourState() {
    }

    NeighbourState(NeighbourState toCopy) {
        // copy constructor
        for (DhtPeerAddress peer : toCopy.successors)
            successors.add(new DhtPeerAddress(peer));

        for (DhtPeerAddress peer : toCopy.predecessors)
            predecessors.add(new DhtPeerAddress(peer));
    }

    synchronized void setLocalAddress(DhtPeerAddress localAddress) {
        this.localAddress = localAddress;
    }

    public synchronized ArrayList<DhtPeerAddress> getPredecessors() {
        ArrayList<DhtPeerAddress> ret = new ArrayList<>(predecessors);
        for (int i=0; i<ret.size()/2; i++) {
            DhtPeerAddress temp = ret.get(i);
            ret.set(i, ret.get(ret.size()-i-1));
            ret.set(ret.size()-i-1, temp);
        }

        return ret;
    }

    public synchronized ArrayList<DhtPeerAddress> getSuccessors() {
        ArrayList<DhtPeerAddress> ret = new ArrayList<>(successors);
        return ret;
    }

    public synchronized TreeSet<DhtPeerAddress> getNeighbours() {
        TreeSet<DhtPeerAddress> peers = new TreeSet<>();
        for (DhtPeerAddress peer : predecessors) {
            DhtPeerAddress neutralPeer = new DhtPeerAddress(peer.getUserID(),
                    peer.getHost(), peer.getPort(), localAddress.getUserID());
            peers.add(neutralPeer);
        }
        for (DhtPeerAddress peer : successors) {
            DhtPeerAddress neutralPeer = new DhtPeerAddress(peer.getUserID(),
                    peer.getHost(), peer.getPort(), localAddress.getUserID());
            peers.add(neutralPeer);
        }
        return peers;
    }

    public synchronized boolean isPredecessor(DhtPeerAddress peer) {
        return predecessors.contains(peer);
    }

    public NeighbourState(DhtPeerAddress localAddress,
                          TreeSet<DhtPeerAddress> successors,
                          TreeSet<DhtPeerAddress> predecessors) {
        this.localAddress = localAddress;
        this.successors = successors;
        this.predecessors = predecessors;
    }
    public  NeighbourState(DhtPeerAddress localAddress) {
        this.localAddress = localAddress;
        this.successors = new TreeSet<>();
        this.predecessors = new TreeSet<>();
    }

    public synchronized boolean isClose (DhtPeerAddress item) {
        // returns whether item is within k hops and at least one hop away
        if (item.equals(localAddress)) return false;
        item.setRelative(localAddress.getUserID());

        return (successors.size() < k || predecessors.size() < k ||
                successors.last().compareTo(item) >= 0 ||
                predecessors.first().compareTo(item) <= 0);
    }

    public synchronized void addNeighbour(DhtPeerAddress item) {
        if (item.equals(localAddress)) return;
        item.setRelative(localAddress.getUserID());

        if (!successors.contains(item)) {
            if (successors.size() < k) {
                successors.add(item);
                if (debug) System.out.println("successor added: " + item.getHost());
            } else if (item.isBetween(localAddress, successors.last())) {
                successors.add(item);
                successors.remove(successors.last());
                if (debug) System.out.println("successor added: " + item.getHost());
            }

            if (debug) {
                System.out.print("successors: ");
                for (DhtPeerAddress p : successors)
                    System.out.print(p.getConnectAddress() + " ");
                System.out.println(" - " + successors.first().relativeSort +
                        " --- " + item.getConnectAddress());
            }
        }

        if (!predecessors.contains(item)) {
            if (predecessors.size() < k) {
                predecessors.add(item);
                if (debug) System.out.println("predecessor added: " + item.getHost());
            } else if (item.isBetween(predecessors.first(), localAddress)) {
                predecessors.add(item);
                predecessors.remove(predecessors.first());
                if (debug) System.out.println("predecessor added: " + item.getHost());
            }

            if (debug) {
                System.out.print("predecessors: ");
                for (DhtPeerAddress p : predecessors)
                    System.out.print(p.getConnectAddress() + " ");
                System.out.println(" - " + predecessors.first().relativeSort +
                        " --- " + item.getConnectAddress());
            }
        }
    }

    public synchronized void print(PrintStream out, String beginning) {
        out.println(beginning + "Predecessors:");
        for (DhtPeerAddress address : predecessors) {
            address.print(out, beginning + "  ");
        }
        out.println(beginning + "Successors:");
        for (DhtPeerAddress address : successors) {
            address.print(out, beginning + "  ");
        }
    }
}
