package message;

import java.util.HashMap;

import statuscodes.RegisterStatus;
import statuscodes.RequestKind;

public class RegisterMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final RegisterStatus status;

    public RegisterMessage(RegisterStatus status, HashMap<String, String> headers, String from) {
        super(RequestKind.Register, headers, from);
        this.status = status;
    }

    // Accessors
    public RegisterStatus getStatus() {
        return this.status;
    }
}
