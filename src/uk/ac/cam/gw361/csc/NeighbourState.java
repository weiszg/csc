package uk.ac.cam.gw361.csc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by gellert on 02/11/2015.
 */
public class NeighbourState implements Serializable {
    public static final int k = 5;
    private boolean debug = false;
    final DhtPeerAddress localAddress;
    private TreeSet<DhtPeerAddress> successors = new TreeSet<>();
    private TreeSet<DhtPeerAddress> predecessors = new TreeSet<>();

    public synchronized ArrayList<DhtPeerAddress> getPredecessors() {
        ArrayList<DhtPeerAddress> preds = new ArrayList<>(predecessors);
        ArrayList<DhtPeerAddress> ret = new ArrayList<>();
        int i = 0;
        while (i<preds.size() && preds.get(i).compareTo(localAddress) <= 0)
            i++;
        i--;
        for (int j=0; j<preds.size(); j++) {
            int index = (i-j)%preds.size();
            if (index<0) index += preds.size();
            ret.add(preds.get(index));
        }
        return ret;
    }
    public synchronized ArrayList<DhtPeerAddress> getSuccessors() {
        ArrayList<DhtPeerAddress> succs = new ArrayList<>(successors);
        ArrayList<DhtPeerAddress> ret = new ArrayList<>();
        int i = 0;
        while (i<succs.size() && succs.get(i).compareTo(localAddress) < 0)
            i++;
        for (int j=0; j<succs.size(); j++) {
            int index = (i+j)%succs.size();
            if (index<0) index += succs.size();
            ret.add(succs.get(index));
        }
        return ret;
    }
    public synchronized TreeSet<DhtPeerAddress> getNeighbours() {
        TreeSet<DhtPeerAddress> neighbours = new TreeSet<>();
        neighbours.addAll(predecessors);
        neighbours.addAll(successors);
        return neighbours;
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
                    System.out.print(p.getPort() + " ");
                System.out.println(" - " + successors.first().relativeSort +
                        " --- " + item.getPort());
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
                    System.out.print(p.getPort() + " ");
                System.out.println(" --- " + item.getPort());
            }
        }
    }

    public synchronized void mergeNeighbourState(NeighbourState toMerge) {
        for (DhtPeerAddress item: toMerge.successors)
            addNeighbour(item);
        for (DhtPeerAddress item: toMerge.predecessors)
            addNeighbour(item);
    }

    public synchronized void cut() {
        TreeSet<DhtPeerAddress> cutSuccessors = new TreeSet<>();
        TreeSet<DhtPeerAddress> cutPredecessors = new TreeSet<>();

        Iterator<DhtPeerAddress> it = successors.iterator();
        int i = 0;
        while (i < k && it.hasNext()) {
            cutSuccessors.add(it.next());
            i++;
        }

        it = predecessors.iterator();
        i = 0;
        while (i < k && it.hasNext()) {
            cutPredecessors.add(it.next());
            i++;
        }

        successors = cutSuccessors;
        predecessors = cutPredecessors;
    }

    public synchronized void print(String beginning) {
        System.out.println(beginning + "Predecessors:");
        for (DhtPeerAddress address : predecessors) {
            if (debug) address.print(beginning + "  ");
        }
        System.out.println(beginning + "Successors:");
        for (DhtPeerAddress address : successors) {
            if (debug) address.print(beginning + "  ");
        }
    }
}
