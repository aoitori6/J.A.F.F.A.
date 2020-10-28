package authserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

import message.*;
import misc.FileInfo;
import statuscodes.DeleteStatus;
import statuscodes.DownloadStatus;
import statuscodes.ErrorStatus;
import statuscodes.FileDetailsStatus;
import statuscodes.LocateServerStatus;
import statuscodes.LoginStatus;
import statuscodes.LogoutStatus;
import statuscodes.RegisterStatus;
import statuscodes.UploadStatus;

final public class AuthServerHandler implements Runnable {
    private final Socket clientSocket;
    private static Connection connection;
    private static Socket primaryFileServerSocket;

    AuthServerHandler(Socket clientSocket, Connection connection, Socket primaryFileServerSocket) {
        this.clientSocket = clientSocket;
        AuthServerHandler.connection = connection;
        AuthServerHandler.primaryFileServerSocket = primaryFileServerSocket;
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
                case Logout:
                    logoutUser((LogoutMessage) request);
                    break;
                case Download:
                    downloadRequest((DownloadMessage) request);
                    break;
                case Upload:
                    uploadRequest((UploadMessage) request);
                    break;
                case Delete:
                    deleteFileRequest((DeleteMessage) request);
                    break;
                case FileDetails:
                    getAllFileDataRequest((FileDetailsMessage) request);
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
        // Generate a random 5-char alphanumeric String
        // 97 corresponds to 'a' and 122 to 'z'
        String tempAuthID = new Random().ints(97, 122 + 1).limit(5)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

        return tempAuthID;
    }

    private static synchronized String hashPassword(String password) {
        byte[] hash = null;
        try {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        } 
        
        StringBuilder hashedPassword = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if(hex.length() == 1) {
                hashedPassword.append('0');
            }
            hashedPassword.append(hex);
        }
        return hashedPassword.toString();
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
                    // Hashing the password
                    String hashedPassword = hashPassword(request.getHeaders().get("pass"));

                    // Create and execute query
                    statement = connection.prepareStatement(query);
                    statement.setString(1, request.getSender());
                    statement.setString(2, hashedPassword);
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
                    String passwordInDB = queryResp.getString("Password");
                    String hashedUserInputPassword = hashPassword((request.getHeaders().get("pass")));
                    if (passwordInDB.equals(hashedUserInputPassword)) {
                        // Password entered by the user matches with the password in the database
                        // Updating user's login status
                        String updateQuery = "UPDATE client SET login_status = 'ONLINE' WHERE username = ?;";
                        PreparedStatement updateStatus = connection.prepareStatement(updateQuery);
                        updateStatus.setString(1, request.getSender());
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
     * Takes a LogoutMessage object from the Client and tries to log the user out
     * 
     * @param request LogoutMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @receivedInstructionIDs: LOGOUT_REQUEST
     * @sentInstructionIDs: LOGOUT_SUCCESS, LOGOUT_FAIL
     */

    private void logoutUser(LogoutMessage request) {

        // Check If Valid Logout request
        if (request.getStatus() == LogoutStatus.LOGOUT_REQUEST) {

            String updateQuery;
            PreparedStatement updateStatus;
            try {
                // Updating the client's login status to OFFLINE
                updateQuery = "UPDATE client SET login_status = 'OFFLINE' WHERE username = ?;";
                updateStatus = connection.prepareStatement(updateQuery);
                updateStatus.setString(1, request.getSender());
                updateStatus.executeUpdate();

                // Sending success response to client
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_SUCCESS, null, "Auth Server"));
            } catch (SQLException e) {
                e.printStackTrace();
                // Sending failure response to the client
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_FAIL, null, "Auth Server"));
                return;
            } finally {
                updateQuery = null;
                updateStatus = null;
            }
        }

        else {
            // Invalid logout request, Sending failure response to the client
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LogoutMessage(LogoutStatus.LOGOUT_FAIL, null, "Auth Server"));
            return;
        }
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

    /**
     * Takes a DownloadMessage object from the Client and sends a DownloadRequest to
     * the Primary File Server. It returns address and port of a Replica File Server
     * on receiving a success message from the Primary File Server
     * 
     * @param request DownloadMessage object from the Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DOWNLOAD_REQUEST
     * @sentInstructionIDs: DOWNLOAD_REQUEST_VALID, DOWNLOAD_REQUEST_INVALID,
     *                      DOWNLOAD_REQUEST_FAIL
     * @sentHeaders: addr:AddressOfReplicaServer, port:PortOfReplicaServer
     */
    private void downloadRequest(DownloadMessage request) {
        // TODO: check the auth token

        if (request.getStatus() == DownloadStatus.DOWNLOAD_REQUEST) {
            // Send a download request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(AuthServerHandler.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_FAIL, null, "Auth Server", null));
                return;
            }

            // Getting the response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(AuthServerHandler.primaryFileServerSocket);
            DownloadMessage castResponse = (DownloadMessage) response;
            response = null;

            if (castResponse.getStatus() != DownloadStatus.DOWNLOAD_REQUEST_VALID) {
                // If Primary File Server returned failure
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_FAIL, null, "Auth Server", null));
                return;
            } else {
                // If Primary File Server returned Success
                HashMap<String, String> headers = new HashMap<String, String>();
                // TODO: return proper Replica File Server details
                headers.put("addr", "localhost");
                headers.put("port", "7689");
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_VALID, headers, "Auth Server", null));
                return;
            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_INVALID, null, "Auth Server", null));
            return;
        }
    }

    /**
     * Takes a UploadMessage object from the Client and sends a UploadRequest to the
     * Primary File Server. On receiving a success message from Primary File Server,
     * it transfers the file uploaded by the client to the Primary File Server
     * 
     * @param request UploadMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: UPLOAD_REQUEST
     * @sentInstructionIDs: UPLOAD_START, UPLOAD_FAIL
     * @sentHeaders: code:code
     */
    private void uploadRequest(UploadMessage request) {
        // TODO: check the auth token

        if (request.getStatus() == UploadStatus.UPLOAD_REQUEST) {
            // Send a upload request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(AuthServerHandler.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "Auth Server", null, null));
                return;
            }

            // Getting the response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(AuthServerHandler.primaryFileServerSocket);
            UploadMessage castResponse = (UploadMessage) response;
            response = null;

            if (castResponse.getStatus() != UploadStatus.UPLOAD_START) {
                // If Primary File Server returned failure
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "Auth Server", null, null));
                return;
            } else {
                // If Primary File Server returned success
                String code = castResponse.getFileInfo().getCode();
                HashMap<String, String> headers = new HashMap<String, String>();
                headers.put("code", code);

                // Sending UploadStart message to the client
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_START, headers, "Auth Server", null, null));

                int buffSize = 1_048_576;
                byte[] writeBuffer = new byte[buffSize];
                BufferedInputStream fileFromClient = null;
                BufferedOutputStream fileToServer = null;
                try {
                    fileFromClient = new BufferedInputStream(this.clientSocket.getInputStream());
                    fileToServer = new BufferedOutputStream(primaryFileServerSocket.getOutputStream());

                    // Temporary var to keep track of total bytes read
                    int _temp_t = 0;
                    // Temporary var to keep track of read Bytes
                    int _temp_c;
                    while ((_temp_c = fileFromClient.read(writeBuffer, 0, writeBuffer.length)) != -1
                            || (_temp_t <= request.getFileInfo().getSize())) {
                        fileToServer.write(writeBuffer, 0, _temp_c);
                        fileToServer.flush();
                        _temp_t += _temp_c;
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                } finally {
                    writeBuffer = null;
                    try {
                        fileFromClient.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "Auth Server", null, null));
            return;
        }
    }

    /**
     * Takes a DeleteMesage object from the Client and sends a DeleteRequest to the
     * Primary File Server. On receiving a success message from the Primary File
     * Server, it sends a DeleteRequest to all the Replica File Servers
     * 
     * @param request DeleteMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DELETE_REQUEST
     * @sentInstructionIDs: DELETE_SUCCESS, DELETE_FAIL, DELETE_INVALID
     */
    private void deleteFileRequest(DeleteMessage request) {
        // TODO: check auth token

        if (request.getStatus() == DeleteStatus.DELETE_REQUEST) {
            // Send a delete request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(AuthServerHandler.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_FAIL, null, "Auth Server", null, false));
                return;
            }

            // Getting the response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(AuthServerHandler.primaryFileServerSocket);
            DeleteMessage castResponse = (DeleteMessage) response;
            response = null;

            if (castResponse.getStatus() != DeleteStatus.DELETE_SUCCESS) {
                // If Primary File Server returned failure
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_FAIL, null, "Auth Server", null, false));
                return;
            } else {
                // If Primary File Server returned success
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_SUCCESS, null, "Auth Server", null, false));

                // TODO: Send deletion request to all the Replica File Servers
                Socket replicaServerSocket = null;
                try {
                    replicaServerSocket = new Socket("localhost", 7689);
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
                MessageHelpers.sendMessageTo(replicaServerSocket, request);
            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DeleteMessage(DeleteStatus.DELETE_INVALID, null, "Auth Server", null, false));
            return;
        }
    }

    /**
     * Takes a FileDetailsMessage object from the Admin and sends a
     * FileDetailsRequest to the Primary File Server. On receiving a success message
     * from the Primary File Server, it recieves the details of all the files from
     * the Primary File Server and transfers it to the Admin
     * 
     * @param request FileDetailsMessage received from the Admin
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: FILEDETAILS_REQUEST
     * @sentInstructionIDs: FILEDETAILS_START, FILEDETAILS_FAIL
     * @sentHeaders: count:FileCount, timestamp:ServerTimestamp (at which time data
     *               was fetched)
     */
    private void getAllFileDataRequest(FileDetailsMessage request) {

        if (request.getStatus() == FileDetailsStatus.FILEDETAILS_REQUEST) {
            // Send a getAllFileDetails request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(AuthServerHandler.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, "Auth Server", null));
                return;
            }

            // Receiving response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(AuthServerHandler.primaryFileServerSocket);
            FileDetailsMessage castResponse = (FileDetailsMessage) response;
            response = null;

            if (castResponse.getStatus() != FileDetailsStatus.FILEDETAILS_START) {
                // If Primary File Server returned failure
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, "Auth Server", null));
                return;
            }

            // If Primary File Server returned success
            // Sending a FileDetails Start message to the client
            MessageHelpers.sendMessageTo(this.clientSocket, new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_START,
                    castResponse.getHeaders(), "Auth Server", null));

            int count = Integer.parseInt(castResponse.getHeaders().get("count"));
            ObjectOutputStream toClient = null;
            ObjectInputStream fromPrimaryServer = null;
            try {
                toClient = new ObjectOutputStream(this.clientSocket.getOutputStream());
                fromPrimaryServer = new ObjectInputStream(AuthServerHandler.primaryFileServerSocket.getInputStream());

                for (int i = 0; i < count; ++i)
                    toClient.writeObject((FileInfo) fromPrimaryServer.readObject());

            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                toClient = null;
                fromPrimaryServer = null;
            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, "Auth Server", null));
            return;
        }
    }
}
