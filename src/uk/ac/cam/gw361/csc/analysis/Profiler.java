package uk.ac.cam.gw361.csc.analysis;

/**
 * Created by gellert on 11/01/2016.
 */
public class Profiler {
    private long startTime;
    private String moduleName;
    private long criticalTimeLength;

    public Profiler(String moduleName, long criticalTimeLength) {
        this.moduleName = moduleName;
        this.criticalTimeLength = criticalTimeLength;
        startTime = System.nanoTime();
    }

    public void end() {
        long endTime = System.nanoTime();
        long elapsedTime = (endTime - startTime) / 1000000;
        String msg = "Profiling: module " + moduleName + " executed for " + elapsedTime + " ms";
        if (elapsedTime >= criticalTimeLength)
            System.err.println(msg);
        else
            System.out.println(msg);
    }
}
