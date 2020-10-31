package signals.derived.unmakeadmin;

import java.util.HashMap;

import signals.base.*;

public class UnMakeAdminMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final String adminToClient;
    private final UnMakeAdminStatus status;
    private final String authToken;
    private final boolean isAdmin;

    public UnMakeAdminMessage(UnMakeAdminStatus status, String adminToClient, HashMap<String, String> headers,
            String from, String authToken, boolean isAdmin) {
        super(RequestKind.UnMakeAdmin, headers, from);
        this.status = status;
        this.adminToClient = adminToClient;
        this.authToken = authToken;
        this.isAdmin = isAdmin;
    }

    // Accessors
    public UnMakeAdminStatus getStatus() {
        return this.status;
    }

    public String getOldAdmin() {
        return this.adminToClient;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public boolean checkAdmin() {
        return this.isAdmin;
    }
}
