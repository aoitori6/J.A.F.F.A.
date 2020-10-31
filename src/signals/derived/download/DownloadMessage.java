package signals.derived.download;

import java.util.HashMap;

import signals.base.*;

public class DownloadMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final String code;
    private final DownloadStatus status;
    private final String authToken;

    public DownloadMessage(DownloadStatus status, String code, HashMap<String, String> headers, String from,
            String authToken) {
        super(RequestKind.Download, headers, from);
        this.status = status;
        this.code = code;
        this.authToken = authToken;
    }

    // Accessors
    public DownloadStatus getStatus() {
        return this.status;
    }

    public String getCode() {
        return this.code;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}
