package signals.derived.auth;

import java.util.HashMap;

import signals.base.*;

public class AuthMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final AuthStatus status;
    private final String clientName;
    private final String authToken;
    private final boolean isClientAdmin;

    public AuthMessage(AuthStatus status, HashMap<String, String> headers, String from, String clientName,
            String authToken, boolean isClientAdmin) {
        super(RequestKind.Upload, headers, from);
        this.status = status;
        this.clientName = clientName;
        this.authToken = authToken;
        this.isClientAdmin = isClientAdmin;
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

    public boolean getIfAdmin() {
        return this.isClientAdmin;
    }
}
