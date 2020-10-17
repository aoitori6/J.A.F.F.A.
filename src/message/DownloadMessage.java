package message;

import java.util.HashMap;

import statuscodes.RequestKind;
import statuscodes.DownloadStatus;

public class DownloadMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final DownloadStatus status;
    private final String authToken;

    public DownloadMessage(DownloadStatus status, HashMap<String, String> headers, String from, String authToken) {
        super(RequestKind.Download, headers, from);
        this.status = status;
        this.authToken = authToken;
    }

    // Accessors
    public DownloadStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}
