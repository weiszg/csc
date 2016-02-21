package uk.ac.cam.gw361.csc.dht;

import java.io.Serializable;
import java.security.SignedObject;

/**
 * Created by gellert on 21/02/2016.
 */
public class SignedFileList implements Serializable {
    // This is a wrapper for SignedObject that also includes a timestamp of when the FileList
    // has last been modified.

    private SignedObject signedObject;
    private long lastModified;

    public SignedFileList(SignedObject signedObject, long lastModified) {
        this.signedObject = signedObject;
        this.lastModified = lastModified;
    }

    public SignedObject getSignedObject() { return signedObject; }
    public long getLastModified() { return lastModified; }
}
