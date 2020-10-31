package signals.derived.syncupload;

import java.util.HashMap;

import misc.FileInfo;
import signals.base.*;

public class SyncUploadMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final SyncUploadStatus status;
    private final String authToken;
    private final String fileCode;
    private final FileInfo fileInfo;

    public SyncUploadMessage(SyncUploadStatus status, HashMap<String, String> headers, String from, String authToken,
            String fileCode, FileInfo fileInfo) {
        super(RequestKind.SyncUpload, headers, from);
        this.status = status;
        this.authToken = authToken;
        this.fileCode = fileCode;
        this.fileInfo = fileInfo;
    }

    // Accessors
    public SyncUploadStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public String getFileCode() {
        return this.fileCode;
    }

    public FileInfo getFileInfo() {
        return this.fileInfo;
    }
}
