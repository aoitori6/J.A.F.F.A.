package message;

import java.util.HashMap;

import statuscodes.LocateServerStatus;
import statuscodes.RequestKind;

public class LocateServerMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final LocateServerStatus status;
    private final String authToken;
    private final boolean isDownloadRequest;

    public LocateServerMessage(LocateServerStatus status, HashMap<String, String> headers, String from,
            String authToken, boolean isDownloadRequest) {
        super(RequestKind.LocateServer, headers, from);
        this.status = status;
        this.authToken = authToken;
        this.isDownloadRequest = isDownloadRequest;
    }

    // Accessors
    public LocateServerStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public boolean checkDownloadRequest() {
        return this.isDownloadRequest;
    }
}
