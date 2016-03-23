package uk.ac.cam.gw361.csc.transfer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by gellert on 23/03/2016.
 */
public class Limiter {
    private final String limitWrite = "", limitRead = "";
    public long ratelimit = 0;
    public long lastSendTimestamp = 0;
    public long lastRecTimestamp = 0;
    public long bytesSent = 0;
    public long bytesReceived = 0;

    private void sleepMillis(long millis) {
        if (millis < 0) return;
        try { Thread.sleep(millis); } catch (InterruptedException e) { }
    }

    public void resetRead(long time) {
        synchronized (limitRead) {
            if (time >= lastRecTimestamp + 1000) {
                lastRecTimestamp = time;
                bytesReceived = 0;
            }
        }
    }

    public void resetWrite(long time) {
        synchronized (limitWrite) {
            if (time >= lastSendTimestamp + 1000) {
                lastSendTimestamp = time;
                bytesSent = 0;
            }
        }
    }

    int limitedRead(InputStream inputStream, byte[] data,
                                   int offset, int length) throws IOException {
        int allowedRead;
        synchronized (limitRead) {
            long time = System.currentTimeMillis();
            resetRead(time);
            if (ratelimit > 0) {
                if (bytesReceived >= ratelimit / 1) {
                    sleepMillis(lastRecTimestamp + 1000 - time);
                    long newTime = System.currentTimeMillis();
                    lastRecTimestamp = newTime;
                    bytesReceived = 0;
                }
            }
            allowedRead = (ratelimit == 0) ? length :
                    (int)Math.min(ratelimit - bytesReceived, length);
            bytesReceived += length;
        }

        int ct = inputStream.read(data, offset, allowedRead);
        return ct;
    }

    void limitedWrite(OutputStream outputStream, byte[] data, int length)
            throws IOException {
        synchronized (limitWrite) {
            long time = System.currentTimeMillis();
            resetWrite(time);

            if (ratelimit > 0) {
                time = System.currentTimeMillis();
                if (bytesSent + length > ratelimit / 1) {
                    sleepMillis(lastSendTimestamp + 1000 - time);
                    time = System.currentTimeMillis();
                    long newTime = System.currentTimeMillis();
                    lastSendTimestamp = newTime;
                    bytesSent = 0;
                }
            }
            bytesSent += length;
        }

        outputStream.write(data, 0, length);
    }
}
