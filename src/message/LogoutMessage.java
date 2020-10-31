package message;

import java.util.HashMap;

import statuscodes.LogoutStatus;
import statuscodes.RequestKind;

public class LogoutMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final LogoutStatus status;
    private final String authToken;

    public LogoutMessage(LogoutStatus status, String authToken, HashMap<String, String> headers, String from) {
        super(RequestKind.Logout, headers, from);
        this.status = status;
        this.authToken = authToken;
    }

    // Accessors
    public LogoutStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}
