package uk.ac.cam.gw361.csc;

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
    }
}
