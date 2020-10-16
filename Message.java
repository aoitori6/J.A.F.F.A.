import java.io.Serializable;
import java.util.HashMap;

abstract public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final HashMap<String, String> headers;
    private final String from;

    protected Message(HashMap<String, String> headers, String from) {
        this.headers = headers;
        this.from = from;
    }

    // Accessors
    public HashMap<String, String> getHeaders() {
        return this.headers;
    }

    public String getSender() {
        return this.from;
    }
}

class LoginMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final LoginStatus status;

    public LoginMessage(LoginStatus status, HashMap<String, String> headers, String from) {
        super(headers, from);
        this.status = status;
    }

    // Accessors
    public LoginStatus getStatus() {
        return this.status;
    }
}

class RegisterMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final RegisterStatus status;

    public RegisterMessage(RegisterStatus status, HashMap<String, String> headers, String from) {
        super(headers, from);
        this.status = status;
    }

    // Accessors
    public RegisterStatus getStatus() {
        return this.status;
    }
}

class DownloadMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final DownloadStatus status;
    private final String authToken;

    public DownloadMessage(DownloadStatus status, HashMap<String, String> headers, String from, String authToken) {
        super(headers, from);
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

class UploadMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final UploadStatus status;
    private final String authToken;

    public UploadMessage(UploadStatus status, HashMap<String, String> headers, String from, String authToken) {
        super(headers, from);
        this.status = status;
        this.authToken = authToken;
    }

    // Accessors
    public UploadStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}

class LocateServerMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final LocateServerStatus status;
    private final String authToken;
    private final boolean isDownloadRequest;

    public LocateServerMessage(LocateServerStatus status, HashMap<String, String> headers, String from,
            String authToken, boolean isDownloadRequest) {
        super(headers, from);
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