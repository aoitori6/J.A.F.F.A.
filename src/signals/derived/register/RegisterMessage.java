package signals.derived.register;

import java.util.HashMap;

import signals.base.*;

public class RegisterMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final RegisterStatus status;
    private final String password;

    public RegisterMessage(RegisterStatus status, String password, HashMap<String, String> headers, String from) {
        super(RequestKind.Register, headers, from);
        this.status = status;
        this.password = password;
    }

    // Accessors
    public RegisterStatus getStatus() {
        return this.status;
    }

    public String getPassword() {
        return this.password;
    }
}
