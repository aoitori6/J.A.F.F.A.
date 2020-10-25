package message;

import java.util.HashMap;

import statuscodes.DeleteStatus;
import statuscodes.RequestKind;

public class DeleteMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final DeleteStatus status;
    private final String authToken;
    private final boolean isAdmin;

    public DeleteMessage(DeleteStatus status, HashMap<String, String> headers, String from,
            String authToken, boolean isAdmin) {
        super(RequestKind.Delete, headers, from);
        this.status = status;
        this.authToken = authToken;
        this.isAdmin = isAdmin;
    }

    // Accessors
    public DeleteStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public boolean checkAdmin() {
        return this.isAdmin;
    }
}
