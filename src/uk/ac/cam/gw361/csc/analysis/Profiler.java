package uk.ac.cam.gw361.csc.analysis;

import java.io.PrintStream;

/**
 * Created by gellert on 11/01/2016.
 */
public class Profiler {
    private long startTime;
    private Reporter reporter;

    public Profiler(Reporter reporter) {
        this.reporter = reporter;
        startTime = System.nanoTime();
    }

    public void end() {
        long endTime = System.nanoTime();
        Long elapsedTime = (endTime - startTime) / 1000000;

        reporter.report(new String[]{((Long) System.currentTimeMillis()).toString(),
                elapsedTime.toString()});

        reporter.flush();
    }
}
