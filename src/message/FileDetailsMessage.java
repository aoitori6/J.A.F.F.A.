package message;

import java.util.HashMap;

import statuscodes.FileDetailsStatus;
import statuscodes.RequestKind;

public class FileDetailsMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final FileDetailsStatus status;
    private final String authToken;

    public FileDetailsMessage(FileDetailsStatus status, HashMap<String, String> headers, String from,
            String authToken) {
        super(RequestKind.FileDetails, headers, from);
        this.status = status;
        this.authToken = authToken;
    }

    // Accessors
    public FileDetailsStatus getStatus() {
        return this.status;
    }

    public String getAuthToken() {
        return this.authToken;
    }
}
