package authserver;

import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

import message.*;
import statuscodes.ErrorStatus;
import statuscodes.LocateServerStatus;
import statuscodes.LoginStatus;
import statuscodes.RegisterStatus;

final public class AuthServerHandler implements Runnable {
    private final Socket clientSocket;
    private static Connection connection;

    AuthServerHandler(Socket clientSocket, Connection connection) {
        this.clientSocket = clientSocket;
        AuthServerHandler.connection = connection;
    }

    @Override
    /**
     * This method contains the central logic of the Auth Server. It listens for
     * {@code Message} objects from the Client, and handles them as required by
     * looking at the status field.
     */
    public void run() {
        listenLoop: while (true) {
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
                    registerUser((RegisterMessage) request);
                    break;
                default:
                    generateErrorMessage(request);
                    break listenLoop;
            }
        }
    }

    /**
     * Function that generates and sends an ErrorMessage to the Client. Invoked only
     * when a {@code Message} with an invalid RequestKind is received by the Server.
     * Whether a specific RequestKind is invalid or not is dependant on the Server
     * configuration.
     * 
     * @param request Received Message with invalid RequestKind
     */
    private void generateErrorMessage(Message request) {
        MessageHelpers.sendMessageTo(this.clientSocket,
                new ErrorMessage(ErrorStatus.INVALID_TO_AUTH_REQUEST, request.getHeaders(), "Auth Server"));

    }

    /**
     * WARNING: This function is not properly implemented yet since it's use case is
     * not clearly defined. Helper function that generates a random alphanumeric
     * String, to be used as an authToken, and then checks against the database to
     * see if the authToken is unique
     * 
     * @return A String that is guaranteed to be unique against the Auth Database
     *         provided
     */
    private static synchronized String generateUserAuthToken() {
        // Generate a random 10-char alphanumeric String
        // 97 corresponds to 'a' and 122 to 'z'
        String tempAuthID = new Random().ints(97, 122 + 1).limit(10)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

        return tempAuthID;
    }

    /**
     * Takes a RegisterMessage object from the Client and tries to register the user
     * vs a database.
     * 
     * @param request RegisterMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @receivedInstructionIDs: REGISTER_REQUEST
     * @sentInstructionIDs: REGISTER_SUCCESS, REGISTER_FAIL
     * @receivedHeaders: pass:Password
     */
    private void registerUser(RegisterMessage request) {

        if (request.getStatus() == RegisterStatus.REGISTER_REQUEST) {

            // Check Auth DB if username is available
            String query = "SELECT * FROM client WHERE username = ?;";
            PreparedStatement statement;
            ResultSet queryResp = null;
            try {
                // Create and execute query
                statement = connection.prepareStatement(query);
                statement.setString(1, request.getSender());
                queryResp = statement.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new RegisterMessage(RegisterStatus.REGISTER_FAIL, null, "Auth Server"));
                return;
            } finally {
                query = null;
                statement = null;
            }

            // Check Query Results
            try {
                if (queryResp != null && queryResp.next() == false) {
                    // Username is available
                    // Create new user in client DB
                    query = "INSERT INTO client(username, password) VALUES(?,?);";

                    // Create and execute query
                    statement = connection.prepareStatement(query);
                    statement.setString(1, request.getSender());
                    statement.setString(2, request.getHeaders().get("pass"));
                    statement.executeUpdate();

                    // Sending success response to Client
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new RegisterMessage(RegisterStatus.REGISTER_SUCCESS, null, "Auth Server"));
                    return;
                }

                else {
                    // User already exists, sending failure response to Client
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new RegisterMessage(RegisterStatus.REGISTER_FAIL, null, "Auth Server"));
                    return;
                }

            } catch (SQLException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new RegisterMessage(RegisterStatus.REGISTER_FAIL, null, "Auth Server"));
                return;
            } finally {
                query = null;
                statement = null;
            }
        }

        else

        {
            // Invalid login request
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new RegisterMessage(RegisterStatus.REGISTER_FAIL, null, "Auth Server"));
            return;
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
     * @sentInstructionIDs: LOGIN_SUCCESS, LOGIN_FAIL
     * @receivedHeaders: pass:Password
     * @sentHeaders: authToken:authToken (null if LOGIN_FAIL)
     */
    private void logInUser(LoginMessage request) {

        HashMap<String, String> headers = new HashMap<String, String>();

        // Check if valid Login Request
        if (request.getStatus() == LoginStatus.LOGIN_REQUEST) {

            // Begin querying Auth DB with supplied username
            String authenticateQuery = "SELECT * FROM client WHERE username = ?;";
            PreparedStatement findUser;
            ResultSet queryResp = null;
            try {
                // Create and execute query
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

                // Check query results
                if (queryResp != null && queryResp.next() == false) {
                    // User doesn't exist
                    headers.put("authToken", null);
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new LoginMessage(LoginStatus.LOGIN_INVALID, headers, "Auth Server"));
                    return;
                }

                else {
                    // User exists, checking password
                    String password = queryResp.getString("Password");

                    if (password.equals(request.getHeaders().get("pass"))) {
                        // Password entered by the user matches with the password in the database
                        // Updating user's login status
                        String updateQuery = "UPDATE client SET login_status = ONLINE WHERE username = ?;";
                        PreparedStatement updateStatus = connection.prepareStatement(updateQuery);
                        updateStatus.setString(2, request.getSender());
                        updateStatus.executeUpdate();

                        // Sending success response to client
                        // TODO: Generate proper, unique authToken
                        headers.put("authToken", "1");
                        MessageHelpers.sendMessageTo(this.clientSocket,
                                new LoginMessage(LoginStatus.LOGIN_SUCCESS, headers, "Auth Server"));
                    }

                    else {
                        // Implies password entered by the user doesn't match with the password in the
                        // database
                        headers.put("authToken", null);
                        MessageHelpers.sendMessageTo(this.clientSocket,
                                new LoginMessage(LoginStatus.LOGIN_INVALID, headers, "Auth Server"));
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
            // Invalid login request
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
