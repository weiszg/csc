package uk.ac.cam.gw361.csc;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by gellert on 02/11/2015.
 */
public class NeighbourState implements Serializable {
    public static final int k = 5;
    final DhtPeerAddress localAddress;
    private TreeSet<DhtPeerAddress> successors = new TreeSet<>();
    private TreeSet<DhtPeerAddress> predecessors = new TreeSet<>();

    public TreeSet<DhtPeerAddress> getSuccessors() { return successors; }
    public TreeSet<DhtPeerAddress> getPredecessors() { return predecessors; }

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


    public void addNeighbour(DhtPeerAddress item) {
        if (successors.size() < k) {
            successors.add(item);
            System.out.println("successor added: " + item.getHost());
        } else if (item.isBetween(localAddress, successors.last())) {
            successors.add(item);
            System.out.println("successor added: " + item.getHost());
        }

        if (predecessors.size() < k) {
            predecessors.add(item);
            System.out.println("predecessor added: " + item.getHost());
        } else if (item.isBetween(predecessors.first(), localAddress)) {
            predecessors.add(item);
            System.out.println("predecessor added: " + item.getHost());
        }
    }

    public void mergeNeighbourState(NeighbourState toMerge) {
        for (DhtPeerAddress item: toMerge.successors)
            addNeighbour(item);
        for (DhtPeerAddress item: toMerge.predecessors)
            addNeighbour(item);
    }

    public void cut() {
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


}
