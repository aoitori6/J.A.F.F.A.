package message;

import java.util.HashMap;

import statuscodes.LogoutStatus;
import statuscodes.RequestKind;

public class LogoutMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final LogoutStatus status;

    public LogoutMessage(LogoutStatus status, HashMap<String, String> headers, String from) {
        super(RequestKind.Logout, headers, from);
        this.status = status;
    }

    // Accessors
    public LogoutStatus getStatus() {
        return this.status;
    }
}
