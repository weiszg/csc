package uk.ac.cam.gw361.csc;

import java.util.Scanner;

/**
 * Created by gellert on 24/10/2015.
 */
public class LocalListener extends Thread {
    public void run(){
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("yo input yo");
            String readStr = scanner.next();

            System.out.println(readStr);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {}
        }
    }
}
