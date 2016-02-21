package uk.ac.cam.gw361.csc.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
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

    public void start() {
        active = true;
    }

    public void stop() {
        active = false;
        flush();
    }

    void flush() {
        out.flush();
    }
}
