package uk.ac.cam.gw361.csc.analysis;

import java.rmi.RemoteException;

/**
 * Created by gellert on 19/03/2016.
 */
public interface SimulationController {
    public void setSpeed(double speed) throws RemoteException;
}
