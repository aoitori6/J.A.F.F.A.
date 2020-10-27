package message;

import java.util.HashMap;

import misc.FileInfo;
import statuscodes.RequestKind;
import statuscodes.UploadStatus;

public class UploadMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final UploadStatus status;
    private final String authToken;
    private final FileInfo fileInfo;

    public UploadMessage(UploadStatus status, HashMap<String, String> headers, String from, String authToken,
            FileInfo fileInfo) {
        super(RequestKind.Upload, headers, from);
        this.status = status;
        this.authToken = authToken;
        this.fileInfo = fileInfo;
    }

    // Accessors
    public UploadStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public FileInfo getFileInfo() {
        return this.fileInfo;
    }
}
