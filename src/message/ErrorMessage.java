package message;

import java.util.HashMap;

import statuscodes.ErrorStatus;
import statuscodes.RequestKind;

public class ErrorMessage extends Message{
    private static final long serialVersionUID = 1L;
    private final ErrorStatus status;

    public ErrorMessage(ErrorStatus status, HashMap<String, String> headers, String from) {
        super(RequestKind.Error, headers, from);
        this.status = status;
    }

    // Accessors
    public ErrorStatus getStatus() {
        return this.status;
    }
}
