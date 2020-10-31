package signals.derived.makeadmin;

import java.util.HashMap;

import signals.base.*;

public class MakeAdminMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final String clientToAdmin;
    private final MakeAdminStatus status;
    private final String authToken;
    private final boolean isAdmin;

    public MakeAdminMessage(MakeAdminStatus status, String clientToAdmin, HashMap<String, String> headers, String from,
            String authToken, boolean isAdmin) {
        super(RequestKind.MakeAdmin, headers, from);
        this.status = status;
        this.clientToAdmin = clientToAdmin;
        this.authToken = authToken;
        this.isAdmin = isAdmin;
    }

    // Accessors
    public MakeAdminStatus getStatus() {
        return this.status;
    }

    public String getNewAdmin() {
        return this.clientToAdmin;
    }

    public String getAuthToken() {
        return this.authToken;
    }

    public boolean checkAdmin() {
        return this.isAdmin;
    }
}
