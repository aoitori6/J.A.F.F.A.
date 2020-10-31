package signals.derived.ping;

import java.util.HashMap;

import signals.base.*;

public class PingMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final PingStatus status;

    public PingMessage(PingStatus status, HashMap<String, String> headers, String from) {
        super(RequestKind.Ping, headers, from);
        this.status = status;
    }

    // Accessors
    public PingStatus getStatus() {
        return this.status;
    }
}
