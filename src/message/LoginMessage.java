package message;

import java.util.HashMap;

import statuscodes.LoginStatus;
import statuscodes.RequestKind;

public class LoginMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final LoginStatus status;
    private final String password;
    private final boolean isAdmin;
    private final String authToken;

    public LoginMessage(LoginStatus status, String password, boolean isAdmin, String authToken,
            HashMap<String, String> headers, String from) {
        super(RequestKind.Login, headers, from);
        this.status = status;
        this.password = password;
        this.isAdmin = isAdmin;
        this.authToken = authToken;
    }

    // Accessors
    public LoginStatus getStatus() {
        return this.status;
    }

    public String getPassword() {
        return this.password;
    }

    public boolean getIfAdmin() {
        return this.isAdmin;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}