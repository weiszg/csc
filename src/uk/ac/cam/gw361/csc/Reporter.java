package uk.ac.cam.gw361.csc;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;

/**
 * Created by gellert on 18/02/2016.
 */
public abstract class Reporter {
    // used to report various data for analysis
    PrintStream out;
    boolean active = false;

    Reporter(String file) {
        try {
        out = new PrintStream(new FileOutputStream(file));
        } catch (IOException e) {
            System.err.println("Reporter error: " + e.toString());
        }
    }

    void report(String[] args) {
        if (!active)
            return;

        for (String arg : args) {
            out.print(arg + ", ");
        }
        out.println("");
    }

    void start() {
        active = true;
    }

    void stop() {
        active = false;
        flush();
    }

    void flush() {
        out.flush();
    }
}

class HopCountReporter extends Reporter {
    HopCountReporter(String file) {
        super(file);
        active = true;
        report(new String[]{"from", "to", "result", "hops", "fingers used"});
    }

    void report(BigInteger from, BigInteger to, BigInteger result,
                Integer hops, Integer fingerUsed) {
        if (!active)
            return;

        String[] args = new String[]{from.toString(), to.toString(), result.toString(),
                hops.toString(), fingerUsed.toString()};
        report(args);
    }
}
