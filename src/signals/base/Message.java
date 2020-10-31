package signals.base;

import java.io.Serializable;
import java.util.HashMap;

abstract public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final HashMap<String, String> headers;
    private final String from;
    private final RequestKind requestKind;

    protected Message(RequestKind requestKind, HashMap<String, String> headers, String from) {
        this.requestKind = requestKind;
        this.headers = headers;
        this.from = from;
    }

    // Accessors
    public HashMap<String, String> getHeaders() {
        return this.headers;
    }

    public String getSender() {
        return this.from;
    }

    public RequestKind getRequestKind() {
        return this.requestKind;
    }

}