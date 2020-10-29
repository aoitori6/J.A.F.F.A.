package authserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import message.*;
import misc.FileInfo;
import statuscodes.DeleteStatus;
import statuscodes.DownloadStatus;
import statuscodes.ErrorStatus;
import statuscodes.FileDetailsStatus;
import statuscodes.LoginStatus;
import statuscodes.LogoutStatus;
import statuscodes.RegisterStatus;
import statuscodes.SyncDeleteStatus;
import statuscodes.SyncUploadStatus;
import statuscodes.UploadStatus;

final public class AuthServerHandler implements Runnable {
    private final Socket clientSocket;
    private final Connection clientDB;
    private final Socket primaryFileServerSocket;

    private ArrayList<InetSocketAddress> replicaAddrs = new ArrayList<InetSocketAddress>(1);

    AuthServerHandler(Socket clientSocket, Connection clientDB, Socket primaryFileServerSocket,
            ArrayList<InetSocketAddress> replicaAddrs) {
        this.clientSocket = clientSocket;
        this.clientDB = clientDB;
        this.primaryFileServerSocket = primaryFileServerSocket;

        this.replicaAddrs = replicaAddrs;
    }

    @Override
    /**
     * This method contains the central logic of the Auth Server. It listens for
     * {@code Message} objects from the Client, and handles them as required by
     * looking at the status field.
     */
    public void run() {
        listenLoop: while (!this.clientSocket.isClosed()) {
            // Expect a Message from the Client
            Message request = MessageHelpers.receiveMessageFrom(this.clientSocket);

            // Central Logic
            // Execute different methods after checking Message status

            switch (request.getRequestKind()) {
                case Login:
                    logInUser((LoginMessage) request);
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
     * Helper function that generates a random alphanumeric String, to be used as an
     * authToken, and then checks against the database to see if the authToken is
     * unique
     * 
     * @return A String that is guaranteed to be unique against the Auth Database
     *         provided
     */
    private static synchronized String generateUserAuthToken(Connection clientDB) throws SQLException {
        String tempAuthID;
        boolean isValidToken = false;
        do {
            // Generate a random 5-char alphanumeric String
            // 97 corresponds to 'a' and 122 to 'z
            tempAuthID = new Random().ints(97, 122 + 1).limit(5)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

            // Query DB to see if String is unique
            PreparedStatement query = clientDB.prepareStatement("SELECT Code from client WHERE Code = ?");
            query.setString(1, tempAuthID);
            if (query.executeUpdate() != 0)
                isValidToken = true;
            query.close();

        } while (isValidToken);
        clientDB.commit();

        return tempAuthID;
    }

    /**
     * Helper function that takes the Client's password and hashes it with SHA-256
     * for storage in the associated DB.
     * 
     * @param password Password received from the Client
     * @return SHA-256 Hash of the password as a Hex String
     */
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
            if (hex.length() == 1) {
                hashedPassword.append('0');
            }
            hashedPassword.append(hex);
        }
        return hashedPassword.toString();
    }

    /**
     * Takes a RegisterMessage object from the Client and tries to register the user
     * on the associated Client DB.
     * 
     * @param request RegisterMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @receivedInstructionIDs: REGISTER_REQUEST
     * @sentInstructionIDs: REGISTER_SUCCESS, REGISTER_REQUEST_FAIL,
     *                      REGISTER_REQUEST_INVALID
     * @receivedHeaders: pass:Password
     */
    private void registerUser(RegisterMessage request) {

        if (request.getStatus() == RegisterStatus.REGISTER_REQUEST) {

            boolean nameValid = true;
            // Check Auth DB if username is available
            try (PreparedStatement query = this.clientDB
                    .prepareStatement("SELECT Username FROM client WHERE Username =?");) {

                query.setString(1, request.getSender());
                // If true, then username is already taken
                if (!query.executeQuery().next())
                    nameValid = false;
                this.clientDB.commit();

            } catch (SQLException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new RegisterMessage(RegisterStatus.REGISTER_FAIL, null, "Auth Server"));
                return;
            }

            // Check Query Results
            if (nameValid) {
                // If username is available, create new user in client DB with supplied password
                // Hashing password
                String hashedPass = AuthServerHandler.hashPassword(request.getHeaders().get("pass"));
                try (PreparedStatement query = this.clientDB
                        .prepareStatement("INSERT INTO client(Username, Password) VALUES (?,?)")) {

                    // Create and execute query
                    query.setString(1, request.getSender());
                    query.setString(2, hashedPass);

                    // Checking if entry was inserted successfully
                    if (query.executeUpdate() == 1)
                        this.clientDB.commit();
                    else {
                        this.clientDB.rollback();
                        throw new IllegalArgumentException("ERROR! Unable to Register new Client!");
                    }

                    // Sending success response to Client
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new RegisterMessage(RegisterStatus.REGISTER_SUCCESS, null, "Auth Server"));
                    return;

                } catch (SQLException | IllegalArgumentException e) {
                    e.printStackTrace();
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new RegisterMessage(RegisterStatus.REGISTER_REQUEST_FAIL, null, "Auth Server"));
                    return;
                }
            }

            else {
                // User already exists, sending failure response to Client
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new RegisterMessage(RegisterStatus.REGISTER_REQUEST_INVALID, null, "Auth Server"));
                return;
            }
        }

        else {
            // Invalid login request
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new RegisterMessage(RegisterStatus.REGISTER_REQUEST_FAIL, null, "Auth Server"));
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

        HashMap<String, String> headers = new HashMap<String, String>(1);

        // Check if valid Login Request
        if (request.getStatus() == LoginStatus.LOGIN_REQUEST) {

            // Generate Auth Token first
            String authToken = null;
            try {
                authToken = AuthServerHandler.generateUserAuthToken(clientDB);
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            } finally {
                if (authToken == null) {
                    headers.put("authToken", null);
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new LoginMessage(LoginStatus.LOGIN_REQUEST_FAIL, headers, "Auth Server"));
                }
            }

            // Try and Update Login_Status as well as Auth Code of the entry with the given
            // username and password.
            // If it succeeds, user has been successfully logged in
            boolean validLogin = true;
            // Hashing password
            String hashedPass = request.getHeaders().get("pass");

            try (PreparedStatement query = this.clientDB
                    .prepareStatement("UPDATE client_database.client SET Login_Status = 'ONLINE', Auth_Code = ? "
                            + "WHERE (Username = ? AND Password = ?)")) {

                query.setString(1, authToken);
                query.setString(2, request.getSender());
                query.setString(3, hashedPass);

                if (query.executeUpdate() != 1) {
                    validLogin = false;
                    this.clientDB.rollback();
                }

                else
                    this.clientDB.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                headers.put("authToken", null);
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LoginMessage(LoginStatus.LOGIN_REQUEST_FAIL, null, "Auth Server"));
                return;
            }

            if (validLogin) {
                headers.put("authToken", authToken);
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LoginMessage(LoginStatus.LOGIN_SUCCESS, headers, "Auth Server"));
            } else {
                headers.put("authToken", null);
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LoginMessage(LoginStatus.LOGIN_REQUEST_INVALID, headers, "Auth Server"));
            }

        } else {
            // Invalid login request
            headers.put("authToken", null);
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LoginMessage(LoginStatus.LOGIN_REQUEST_FAIL, headers, "Auth Server"));
            return;
        }
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
     * @receivedHeaders: authToken:AuthToken
     */

    private void logoutUser(LogoutMessage request) {

        // Check If Valid Logout request
        if (request.getStatus() == LogoutStatus.LOGOUT_REQUEST) {
            boolean loggedOut = false;
            try (PreparedStatement query = this.clientDB.prepareStatement(
                    "UPDATE client SET Login_Status = 'OFFLINE', Auth_Code = ? WHERE(Username = ? AND Auth_Code = ?)");) {

                query.setNull(1, Types.NULL);
                query.setString(2, request.getSender());
                query.setString(3, request.getHeaders().get("authToken"));

                if (query.executeUpdate() != 1)
                    this.clientDB.rollback();

                else {
                    this.clientDB.commit();
                    loggedOut = true;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_REQUEST_FAIL, null, "Auth Server"));
                return;
            }

            if (loggedOut)
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_SUCCESS, null, "Auth Server"));
            else
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_REQUEST_INVALID, null, "Auth Server"));
        }

        else {
            // Invalid logout request, Sending failure response to the client
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LogoutMessage(LogoutStatus.LOGOUT_REQUEST_FAIL, null, "Auth Server"));
            return;
        }
    }

    /**
     * Helper function that picks a valid Replica Server that the Client can connect
     * to for downloading.
     * 
     */
    private static synchronized InetSocketAddress locateReplicaServer(ArrayList<InetSocketAddress> replicaAddrs) {
        // TODO: Load balancing with multiple File Servers

        // For now direct it to a single File Server
        return replicaAddrs.get(0);
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
     * @expectedHeaders: code:Code
     * @sentHeaders: addr:AddressOfReplicaServer, port:PortOfReplicaServer
     */
    private void downloadRequest(DownloadMessage request) {
        if (request.getStatus() == DownloadStatus.DOWNLOAD_REQUEST) {

            // Send a download request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(this.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_FAIL, null, null, "Auth Server", null));
                return;
            }

            // Getting the response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(this.primaryFileServerSocket);
            DownloadMessage castResponse = (DownloadMessage) response;
            response = null;

            if (castResponse.getStatus() != DownloadStatus.DOWNLOAD_REQUEST_VALID) {
                // If Primary File Server returned failure
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_INVALID, null, null, "Auth Server", null));
                return;
            } else {
                // If Primary File Server returned Success
                HashMap<String, String> headers = new HashMap<String, String>();
                InetSocketAddress replicaAddr = AuthServerHandler.locateReplicaServer(this.replicaAddrs);
                headers.put("addr", replicaAddr.getHostString());
                headers.put("port", Integer.toString(replicaAddr.getPort()));

                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_VALID, null, headers, "Auth Server", null));
                return;
            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_FAIL, null, null, "Auth Server", null));
            return;
        }
    }

    /**
     * Helper method for uploadRequest that notifies all specified Replica Servers
     * that a File with with a specified code has been pushed to the Primary Server,
     * and that they should pull the same.
     * 
     * <p>
     * It sends a SyncUploadMessage to each Replica in the provided List, and if any
     * of them fail to pull the file, their entries are removed from the List
     * permanently, and the Auth Server will no longer route to them.
     * 
     * @param replicaAddrs List of Replica Servers to notify
     * @param code         Just uploaded File Code
     * 
     *                     <p>
     *                     Message Specs
     * @sentInstructionIDs: SYNCUPLOAD_REQUEST
     * @expectedInstructionIDs: SYNCUPLOAD_SUCCESS, SYNCUPLOAD_FAIL,
     * @sentHeaders: code:Code
     */
    private static synchronized void replicaSyncUpload(ArrayList<InetSocketAddress> replicaAddrs, String code) {
        for (InetSocketAddress replicaAddr : replicaAddrs) {
            try (Socket replicaSocket = new Socket(replicaAddr.getAddress(), replicaAddr.getPort());) {

                // Auth Server will wait around 10 minutes for a response
                replicaSocket.setSoTimeout((int) TimeUnit.MINUTES.toMillis(10));

                // If message sending failed
                if (!MessageHelpers.sendMessageTo(replicaSocket, new SyncUploadMessage(
                        SyncUploadStatus.SYNCUPLOAD_REQUEST, null, "Auth Server", "tempAuthToken", code, null)))
                    throw new Exception();

                // Parsing response
                Message response = (Message) MessageHelpers.receiveMessageFrom(replicaSocket);
                SyncUploadMessage castResponse = (SyncUploadMessage) response;
                response = null;

                if (castResponse.getStatus() == SyncUploadStatus.SYNCUPLOAD_SUCCESS)
                    continue;
                else
                    throw new Exception();

            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("ERROR: Communication Error with Replica Server " + replicaAddr.toString());
                synchronized (replicaAddrs) {
                    replicaAddrs.remove(replicaAddr);
                }
                continue;
            }
        }
    }

    /**
     * Takes a UploadMessage object from the Client and sends a UploadRequest to the
     * Primary File Server. On receiving a success message from Primary File Server,
     * it transfers the file uploaded by the client to the Primary File Server. It
     * then attempts to notify all connected Replica Servers to pull the newly
     * uploaded file from the Primary Server.
     * 
     * @param request UploadMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: UPLOAD_REQUEST
     * @sentInstructionIDs: UPLOAD_REQUEST_INVALID, UPLOAD_START, UPLOAD_FAIL
     * @sentHeaders: code:Code
     */
    private void uploadRequest(UploadMessage request) {
        if (request.getStatus() == UploadStatus.UPLOAD_REQUEST) {
            // Send a upload request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(this.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "Auth Server", null, null));
                return;
            }

            // Getting the response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(this.primaryFileServerSocket);
            UploadMessage castResponse = (UploadMessage) response;
            response = null;

            if (castResponse.getStatus() != UploadStatus.UPLOAD_START) {
                // If Primary File Server returned failure
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "Auth Server", null, null));
                return;
            } else {
                // If Primary File Server returned success
                // Sending UploadStart message to the client
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_START, null, "Auth Server", null, null));

                int buffSize = 1_048_576;
                byte[] writeBuffer = new byte[buffSize];
                BufferedInputStream fileFromClient = null;
                BufferedOutputStream fileToServer = null;
                try {
                    fileFromClient = new BufferedInputStream(this.clientSocket.getInputStream());
                    fileToServer = new BufferedOutputStream(primaryFileServerSocket.getOutputStream());

                    // Temporary var to keep track of total bytes read
                    long _temp_t = 0;
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
                        fileFromClient = null;
                        fileToServer = null;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // Notify each Replica Server to pull the newly uploaded File. If any Replica
                // Server errors in this, remove it from the list
                replicaSyncUpload(replicaAddrs, castResponse.getFileInfo().getCode());

                // Notify Client
                MessageHelpers.sendMessageTo(this.clientSocket, new UploadMessage(UploadStatus.UPLOAD_SUCCESS, null,
                        "Auth Server", "tempAuthToken", castResponse.getFileInfo()));

            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new UploadMessage(UploadStatus.UPLOAD_REQUEST_INVALID, null, "Auth Server", null, null));
            return;
        }
    }

    /**
     * Helper method for deleteFileRequest that notifies all specified Replica
     * Servers that a File with with a specified code has been deleted from the
     * Primary Server, and that they should do the same.
     * 
     * <p>
     * It sends a SyncDeleteMessage to each Replica in the provided List, and if any
     * of them fail to pull the file, their entries are removed from the List
     * permanently, and the Auth Server will no longer route to them.
     * 
     * @param replicaAddrs List of Replica Servers to notify
     * @param code         Just uploaded File Code
     * 
     *                     <p>
     *                     Message Specs
     * @sentInstructionIDs: SYNCDELETE_REQUEST
     * @expectedInstructionIDs: SYNCDELETE_SUCCESS, SYNCDELETE_FAIL,
     * @sentHeaders: code:Code
     */
    private static synchronized void replicaSyncDelete(ArrayList<InetSocketAddress> replicaAddrs, String code) {
        for (InetSocketAddress replicaAddr : replicaAddrs) {
            try (Socket replicaSocket = new Socket(replicaAddr.getAddress(), replicaAddr.getPort());) {

                // Auth Server will wait around 10 minutes for a response
                replicaSocket.setSoTimeout((int) TimeUnit.MINUTES.toMillis(10));

                // If message sending failed
                if (!MessageHelpers.sendMessageTo(replicaSocket, new SyncDeleteMessage(
                        SyncDeleteStatus.SYNCDELETE_REQUEST, null, "Auth Server", "tempAuthToken", code)))
                    throw new Exception();

                // Parsing response
                Message response = (Message) MessageHelpers.receiveMessageFrom(replicaSocket);
                SyncDeleteMessage castResponse = (SyncDeleteMessage) response;
                response = null;

                if (castResponse.getStatus() == SyncDeleteStatus.SYNCDELETE_SUCCESS)
                    continue;
                else
                    throw new Exception();

            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("ERROR: Communication Error with Replica Server " + replicaAddr.toString());
                synchronized (replicaAddrs) {
                    replicaAddrs.remove(replicaAddr);
                }
                continue;
            }
        }
    }

    /**
     * Takes a DeleteMesage object from the Client and sends a DeleteRequest to the
     * Primary File Server. On receiving a success message from the Primary File
     * Server, it sends a SyncDeleteRequest to all the Replica File Servers
     * 
     * @param request DeleteMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DELETE_REQUEST
     * @sentInstructionIDs: DELETE_SUCCESS, DELETE_FAIL, DELETE_INVALID
     */
    private void deleteFileRequest(DeleteMessage request) {
        if (request.getStatus() == DeleteStatus.DELETE_REQUEST) {
            // TODO: Check Auth Token
            // Forward delete request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(this.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_FAIL, null, null, "Auth Server", null, false));
                return;
            }

            // Getting the response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(this.primaryFileServerSocket);
            DeleteMessage castResponse = (DeleteMessage) response;
            response = null;

            if (castResponse.getStatus() != DeleteStatus.DELETE_SUCCESS) {
                // If Primary File Server returned failure
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_FAIL, null, null, "Auth Server", null, false));
                return;
            } else {
                // If Primary File Server returned success
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_SUCCESS, null, null, "Auth Server", null, false));

                // Syncing to Replicas
                AuthServerHandler.replicaSyncDelete(this.replicaAddrs, request.getCode());
            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DeleteMessage(DeleteStatus.DELETE_INVALID, null, null, "Auth Server", null, false));
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
            if (!MessageHelpers.sendMessageTo(this.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, "Auth Server", null));
                return;
            }

            // Receiving response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(this.primaryFileServerSocket);
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
                fromPrimaryServer = new ObjectInputStream(this.primaryFileServerSocket.getInputStream());

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
