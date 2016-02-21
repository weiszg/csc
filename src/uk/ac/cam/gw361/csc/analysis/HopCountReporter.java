package uk.ac.cam.gw361.csc.analysis;

import java.math.BigInteger;

/**
 * Created by gellert on 21/02/2016.
 */

public class HopCountReporter extends Reporter {
    public HopCountReporter(String file) {
        super(file);
        active = true;
        report(new String[]{"from", "to", "result", "hops", "fingers used"});
    }

    public void report(BigInteger from, BigInteger to, BigInteger result,
                Integer hops, Integer fingerUsed) {
        if (!active)
            return;

        String[] args = new String[]{from.toString(), to.toString(), result.toString(),
                hops.toString(), fingerUsed.toString()};
        report(args);
    }
}
