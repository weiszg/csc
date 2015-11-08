package uk.ac.cam.gw361.csc;

import java.util.Scanner;

/**
 * Created by gellert on 01/11/2015.
 */
public class Main {
    public static void main(String[] args) {
        String userName = (args.length < 1) ? null : args[0];
        String host = (args.length < 2) ? null : args[1];
        LocalPeer localPeer = new LocalPeer(userName);
        if (host != null) {
            localPeer.join(host);
        }

        Thread commandReader = new CommandReader(localPeer);
        commandReader.start();
    }
}


class CommandReader extends Thread {
    private LocalPeer localPeer;

    public CommandReader(LocalPeer localPeer) {
        this.localPeer = localPeer;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String readStr = scanner.next();
            if (readStr.equals("nb")) {
                localPeer.getNeighbourState().print("");
            } else if (readStr.equals("stabilise")) {
                localPeer.stabilise();
            }
            else System.out.println("Unrecognised command: " + readStr);
        }
    }
}
