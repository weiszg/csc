package uk.ac.cam.gw361.csc.analysis;

import java.math.BigInteger;

/**
 * Created by gellert on 08/03/2016.
 */

public class NetworkUsageReporter extends Reporter {
    public NetworkUsageReporter(String file) {
        super(file);
        active = true;
        // report(new String[]{"time (ms)", "RMI in", "RMI out", "TCP in", "TCP out"});
    }

    public void report(String[] traffic) {
        if (!active)
            return;

        super.report(traffic);
    }
}
