package message;

import java.util.HashMap;

import statuscodes.LoginStatus;
import statuscodes.RequestKind;

public class LoginMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final LoginStatus status;

    public LoginMessage(LoginStatus status, HashMap<String, String> headers, String from) {
        super(RequestKind.Login, headers, from);
        this.status = status;
    }

    // Accessors
    public LoginStatus getStatus() {
        return this.status;
    }
}