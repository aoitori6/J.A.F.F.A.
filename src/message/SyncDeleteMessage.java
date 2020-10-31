package message;

import java.util.HashMap;

import statuscodes.SyncDeleteStatus;
import statuscodes.RequestKind;

public class SyncDeleteMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final SyncDeleteStatus status;
    private final String authToken;
    private final String fileCode;

    public SyncDeleteMessage(SyncDeleteStatus status, HashMap<String, String> headers, String from, String authToken,
            String fileCode) {
        super(RequestKind.SyncDelete, headers, from);
        this.status = status;
        this.fileCode = fileCode;
        this.authToken = authToken;
    }

    // Accessors
    public SyncDeleteStatus getStatus() {
        return this.status;
    }

    public String getFileCode() {
        return this.fileCode;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}
