package uk.ac.cam.gw361.csc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by gellert on 22/03/2016.
 */
public class Client {
    public static void main(String[] args) {
        // set truststore
        // System.setProperty("javax.net.debug", "all");
        System.setProperty("javax.net.ssl.trustStore", "truststore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        // System.setProperty("javax.net.debug", "all");

        List<String> oldArgs = new ArrayList<>(Arrays.asList(args));
        oldArgs.add("csconly");
        String[] newArgs = new String[oldArgs.size()];
        newArgs = oldArgs.toArray(newArgs);
        Server.manualStart(newArgs);
    }
}
