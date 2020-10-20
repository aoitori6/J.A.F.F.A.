package authserver;

import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import message.*;
import statuscodes.LocateServerStatus;
import statuscodes.LoginStatus;

final public class AuthServerHandler implements Runnable {
    private final Socket clientSocket;
    private final Connection connection;

    AuthServerHandler(Socket clientSocket, Connection connection) {
        this.clientSocket = clientSocket;
        this.connection = connection;
    }

    @Override
    /**
     * This method contains the central logic of the Auth Server. It listens for
     * {@code Message} objects from the Client, and handles them as required by
     * looking at the status field.
     */
    public void run() {
        while(true) { 
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
                    break;
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

        HashMap<String, String> headers = new HashMap<String, String>();

        // Authentication from client_database
        if (request.getStatus() == LoginStatus.LOGIN_REQUEST) {

            // Find the user with the given Username
            String authenticateQuery = "SELECT * FROM client where Username = ?;";
            PreparedStatement findUser;
            ResultSet queryResp = null;
            try{
                //Quering client_database
                findUser = connection.prepareStatement(authenticateQuery);
                findUser.setString(1, request.getSender());
                queryResp = findUser.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            } finally {
                authenticateQuery = null;
                findUser = null;
            }
            
            try {
                headers.clear();

                if (queryResp != null && queryResp.next() == false) {
                    // Implies user doesn't exist
                    headers.put("authToken", null);
                    MessageHelpers.sendMessageTo(this.clientSocket,
                        new LoginMessage(LoginStatus.LOGIN_FAIL, headers, "Auth Server"));
                        return;
                }

                else {
                    // User exists, checking password
                    String password = queryResp.getString("Password");
                    
                    if (password.equals(request.getHeaders().get("pass"))) {
                        // Password entered by the user matches with the password in the database
                        // Updating user's login status
                        String updateQuery = "UPDATE client SET Login_Status = ? where Username = ?;";
                        PreparedStatement updateStatus = connection.prepareStatement(updateQuery);
                        updateStatus.setString(1, "ONLINE");
                        updateStatus.setString(2, request.getSender());
                        updateStatus.executeUpdate();

                        // Sending success response to client
                        headers.put("authToken", "1");
                        MessageHelpers.sendMessageTo(this.clientSocket,
                            new LoginMessage(LoginStatus.LOGIN_SUCCESS, headers, "Auth Server"));
                    }
                    
                    else {
                        // Implies password entered by the user doesn't match with the password in the database
                        headers.put("authToken", null);
                        MessageHelpers.sendMessageTo(this.clientSocket,
                            new LoginMessage(LoginStatus.LOGIN_FAIL, headers, "Auth Server"));
                        return;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            } finally {
                headers = null;
            }
        }

        else {
            // Implies user didn't make a login request
            headers.put("authToken", null);
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LoginMessage(LoginStatus.LOGIN_FAIL, headers, "Auth Server"));
            return;
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
