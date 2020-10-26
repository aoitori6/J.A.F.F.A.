package message;

import java.util.HashMap;

import statuscodes.RequestKind;
import statuscodes.AuthStatus;

public class AuthMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final AuthStatus status;
    private final String clientName;
    private final String authToken;

    public AuthMessage(AuthStatus status, HashMap<String, String> headers, String from, String clientName,
            String authToken) {
        super(RequestKind.Upload, headers, from);
        this.status = status;
        this.clientName = clientName;
        this.authToken = authToken;
    }

    // Accessors
    public String getClientName() {
        return this.clientName;
    }

    public AuthStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}
