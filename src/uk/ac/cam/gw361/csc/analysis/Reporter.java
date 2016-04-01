package uk.ac.cam.gw361.csc.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;

/**
 * Created by gellert on 18/02/2016.
 */
public class Reporter {
    // used to report various data for analysis
    PrintStream out;
    boolean active = false;
    String file;

    public Reporter(PrintStream out) {
        this.out = out;
        active = true;
    }

    private void init() {
        try {
            out = new PrintStream(new FileOutputStream(file));
            active = true;
        } catch (IOException e) {
            System.err.println("Reporter error: " + e.toString());
        }
    }

    public Reporter(String file) {
        this.file = file;
        init();
    }

    public void restart() {
        stop();
        out.close();
        init();
    }

    public void report(String[] args) {
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
