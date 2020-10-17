package authserver;

import java.net.Socket;
import java.util.HashMap;

import message.*;
import statuscodes.LocateServerStatus;
import statuscodes.LoginStatus;

final public class AuthServerHandler implements Runnable {
    private final Socket clientSocket;

    AuthServerHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    /**
     * This method contains the central logic of the Auth Server. It listens for
     * {@code Message} objects from the Client, and handles them as required by
     * looking at the status field.
     */
    public void run() {
        while (true) {
            // Expect a Message from the Client
            Message request = MessageHelpers.receiveMessageFrom(this.clientSocket);

            // Central Logic
            // Execute different methods after checking Message status

            switch (request.getRequestKind()) {
                case Login:
                    logInUser((LoginMessage) request);
                    break;
                case LocateServer:
                    locateFileServer((LocateServerMessage) request);
                    break;
                case Register:
                    break;
                default:
                    return;
            }
        }
    }

    /**
     * Takes a LoginMessage object from the Client and tries to authenticate vs a
     * database.
     * 
     * @param request LoginMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @receivedInstructionIDs: LOGIN_REQUEST
     * @sentHeaders: authToken:authToken (null if LOGIN_FAIL)
     */
    private void logInUser(LoginMessage request) {
        // TODO: Actual authentication from a MySQL DB

        HashMap<String, String> headers = new HashMap<String, String>();

        // For now, simply authorize everyone;
        if (request.getStatus() == LoginStatus.LOGIN_REQUEST) {
            headers.put("authToken", "1");
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LoginMessage(LoginStatus.LOGIN_SUCCESS, headers, "Auth Server"));
        }

        else {
            headers.put("authToken", null);
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LoginMessage(LoginStatus.LOGIN_FAIL, headers, "Auth Server"));
        }

        headers = null;
    }

    /**
     * Takes a LocateServerMessage object from the Client and tries to return a
     * valid File Server.
     * 
     * @param request LocateServerMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @receivedInstructionIDs: GET_SERVER
     * @sentInstructionIDs: SERVER_FOUND, SERVER_NOT_FOUND
     * @sentHeaders: addr:ServerAddress, port:ServerPort
     */
    private void locateFileServer(LocateServerMessage request) {
        // TODO: Load balancing with multiple File Servers

        HashMap<String, String> headers = new HashMap<String, String>();

        // For now direct it to a single File Server
        if (request.getStatus() == LocateServerStatus.GET_SERVER) {
            headers.put("addr", "localhost");
            headers.put("port", "7689");
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LocateServerMessage(LocateServerStatus.SERVER_FOUND, headers, "Auth Server", null, false));
        }

        else {
            headers.put("addr", null);
            headers.put("port", null);
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LocateServerMessage(LocateServerStatus.SERVER_NOT_FOUND, headers, "Auth Server", null, false));
        }
        headers = null;
    }

}
