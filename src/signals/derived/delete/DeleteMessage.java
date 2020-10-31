package signals.derived.delete;

import java.util.HashMap;

import signals.base.*;

public class DeleteMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final String code;
    private final DeleteStatus status;
    private final String authToken;
    private final boolean isAdmin;

    public DeleteMessage(DeleteStatus status, String code, HashMap<String, String> headers, String from,
            String authToken, boolean isAdmin) {
        super(RequestKind.Delete, headers, from);
        this.status = status;
        this.code = code;
        this.authToken = authToken;
        this.isAdmin = isAdmin;
    }

    // Accessors
    public DeleteStatus getStatus() {
        return this.status;
    }

    public String getCode() {
        return this.code;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public boolean checkAdmin() {
        return this.isAdmin;
    }
}
