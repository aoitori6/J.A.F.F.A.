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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import message.*;
import misc.FileInfo;
import statuscodes.*;

final public class AuthServerHandler implements Runnable {
    private final Socket clientSocket;
    private final Connection clientDB;
    private final InetSocketAddress primaryServerAddress;

    private CopyOnWriteArrayList<InetSocketAddress> replicaAddrs;
    private HashMap<InetSocketAddress, InetSocketAddress> replicaAddrsForClient;
    private final ExecutorService clientThreadPool;

    AuthServerHandler(Socket clientSocket, Connection clientDB, InetSocketAddress primaryServerAddress,
            CopyOnWriteArrayList<InetSocketAddress> replicaAddrs,
            HashMap<InetSocketAddress, InetSocketAddress> replicaAddrsForClient, ExecutorService clientThreadPool) {
        this.clientSocket = clientSocket;
        this.clientDB = clientDB;
        this.primaryServerAddress = primaryServerAddress;

        this.replicaAddrs = replicaAddrs;
        this.replicaAddrsForClient = replicaAddrsForClient;

        this.clientThreadPool = clientThreadPool;
    }

    @Override
    /**
     * This method contains the central logic of the Auth Server. It listens for
     * {@code Message} objects from the Client, and handles them as required by
     * looking at the status field.
     */
    public void run() {
        // Expect a Message from the Client
        Message request = MessageHelpers.receiveMessageFrom(this.clientSocket);
        System.out.println(request);
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
                break;
            case MakeAdmin:
                makeUserAdmin((MakeAdminMessage) request);
                break;
            case UnMakeAdmin:
                unMakeUserAdmin((UnMakeAdminMessage) request);
                break;
            default:
                break;
        }

        try {
            this.clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            // 48 is the lower limit and corresponds to 'a', 122 is the upper limit and
            // corresponds to 'Z'
            tempAuthID = new Random().ints(48, 122 + 1).filter((i) -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                    .limit(5).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();

            // Query DB to see if String is unique
            PreparedStatement query = clientDB.prepareStatement("SELECT Auth_Code from client WHERE Auth_Code = ?");
            query.setString(1, tempAuthID);
            ResultSet queryResp = query.executeQuery();

            if (!queryResp.next())
                isValidToken = true;
            query.close();

        } while (!isValidToken);
        clientDB.commit();

        return tempAuthID;
    }

    /**
     * Helper function to check if supplied credentials and Auth Token are valid or
     * not by querying the associated Client DB.
     * 
     * @param client    Client's Name
     * @param authToken Client's Auth Token
     * @param isAdmin   Client's Admin Status
     * @return Hash map with two keys; valid:{@code true} if Auth Token is valid
     *         else {@code false}, isAdmin: {@code true} if Client is admin else
     *         {@code false}
     */
    private HashMap<String, Boolean> checkAuthToken(String client, String authToken) {
        HashMap<String, Boolean> resp = new HashMap<String, Boolean>(2);
        try (PreparedStatement query = this.clientDB
                .prepareStatement("SELECT Admin_Status FROM client WHERE Username = ? AND Login_Status = 'ONLINE'");) {
            query.setString(1, client);
            ResultSet queryResp = query.executeQuery();

            if (queryResp.next()) {
                resp.put("valid", true);
                resp.put("isAdmin", queryResp.getBoolean("Admin_Status"));
            }

            return resp;
        } catch (Exception e) {
            e.printStackTrace();
        }

        resp.put("valid", false);
        resp.put("isAdmin", false);
        return resp;
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
     */
    private void registerUser(RegisterMessage request) {

        if (request.getStatus() == RegisterStatus.REGISTER_REQUEST) {

            boolean nameValid = true;
            // Check Auth DB if username is available
            try (PreparedStatement query = this.clientDB
                    .prepareStatement("SELECT Username FROM client WHERE Username = ?");) {

                query.setString(1, request.getSender());
                // If true, then username is already taken
                if (query.executeQuery().next())
                    nameValid = false;
                this.clientDB.commit();

            } catch (SQLException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new RegisterMessage(RegisterStatus.REGISTER_FAIL, null, null, AuthServer.SERVER_NAME));
                return;
            }

            // Check Query Results
            if (nameValid) {
                // If username is available, create new user in client DB with supplied password
                // Hashing password
                String hashedPass = AuthServerHandler.hashPassword(request.getPassword());
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
                            new RegisterMessage(RegisterStatus.REGISTER_SUCCESS, null, null, AuthServer.SERVER_NAME));
                    return;

                } catch (SQLException | IllegalArgumentException e) {
                    e.printStackTrace();
                    MessageHelpers.sendMessageTo(this.clientSocket, new RegisterMessage(
                            RegisterStatus.REGISTER_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME));
                    return;
                }
            }

            else {
                // User already exists, sending failure response to Client
                MessageHelpers.sendMessageTo(this.clientSocket, new RegisterMessage(
                        RegisterStatus.REGISTER_REQUEST_INVALID, null, null, AuthServer.SERVER_NAME));
                return;
            }
        }

        else {
            // Invalid login request
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new RegisterMessage(RegisterStatus.REGISTER_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME));
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
     */
    private void logInUser(LoginMessage request) {

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
                    MessageHelpers.sendMessageTo(this.clientSocket, new LoginMessage(LoginStatus.LOGIN_REQUEST_FAIL,
                            null, false, null, null, AuthServer.SERVER_NAME));
                }
            }

            // Try and Update Login_Status as well as Auth Code of the entry with the given
            // username and password.
            // If it succeeds, user has been successfully logged in
            boolean validLogin = true;
            // Hashing password
            String hashedPass = AuthServerHandler.hashPassword(request.getPassword());

            try (PreparedStatement query = this.clientDB
                    .prepareStatement("UPDATE client_database.client SET Login_Status = 'ONLINE', Auth_Code = ? "
                            + "WHERE (Username = ? AND Password = ? AND Admin_Status = ?)")) {

                query.setString(1, authToken);
                query.setString(2, request.getSender());
                query.setString(3, hashedPass);
                query.setBoolean(4, request.getIfAdmin());

                if (query.executeUpdate() != 1) {
                    validLogin = false;
                    this.clientDB.rollback();
                }

                else
                    this.clientDB.commit();

            } catch (SQLException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket, new LoginMessage(LoginStatus.LOGIN_REQUEST_FAIL, null,
                        false, null, null, AuthServer.SERVER_NAME));
                return;
            }

            if (validLogin) {
                MessageHelpers.sendMessageTo(this.clientSocket, new LoginMessage(LoginStatus.LOGIN_SUCCESS, null,
                        request.getIfAdmin(), authToken, null, AuthServer.SERVER_NAME));
            } else {
                MessageHelpers.sendMessageTo(this.clientSocket, new LoginMessage(LoginStatus.LOGIN_REQUEST_FAIL, null,
                        false, null, null, AuthServer.SERVER_NAME));
            }

        } else {
            // Invalid login request
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LoginMessage(LoginStatus.LOGIN_REQUEST_FAIL, null, false, AuthServer.SERVER_NAME, null, null));
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
     */

    private void logoutUser(LogoutMessage request) {

        // Check If Valid Logout request
        if (request.getStatus() == LogoutStatus.LOGOUT_REQUEST) {
            boolean loggedOut = false;
            try (PreparedStatement query = this.clientDB.prepareStatement(
                    "UPDATE client SET Login_Status = 'OFFLINE', Auth_Code = ? WHERE Auth_Code = ?");) {

                query.setNull(1, Types.NULL);
                query.setString(2, request.getAuthToken());

                if (query.executeUpdate() != 1)
                    this.clientDB.rollback();

                else {
                    this.clientDB.commit();
                    loggedOut = true;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME));
                return;
            }

            if (loggedOut)
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_SUCCESS, null, null, AuthServer.SERVER_NAME));
            else
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new LogoutMessage(LogoutStatus.LOGOUT_REQUEST_INVALID, null, null, AuthServer.SERVER_NAME));
        }

        else {
            // Invalid logout request, Sending failure response to the client
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new LogoutMessage(LogoutStatus.LOGOUT_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME));
            return;
        }
    }

    /**
     * Helper function that picks a valid Replica Server that the Client can connect
     * to for downloading. Picks a random server to balance out the load.
     * 
     */
    private InetSocketAddress locateReplicaServer() {
        // Find the right server
        int index = ThreadLocalRandom.current().nextInt(this.replicaAddrs.size());
        Iterator<InetSocketAddress> itr = this.replicaAddrs.iterator();

        // If list is empty
        if (!itr.hasNext())
            return null;

        InetSocketAddress addr = null;
        for (int i = 0; i <= index && itr.hasNext(); ++i) {
            addr = itr.next();
            System.out.println(addr);
        }

        // Try pinging it to see if its alive
        try (Socket tempConn = new Socket(addr.getAddress(), addr.getPort());) {
            System.out.println("CONNECTING TO: " + addr);
            MessageHelpers.sendMessageTo(tempConn,
                    new PingMessage(PingStatus.PING_START, null, AuthServer.SERVER_NAME));
            Message received = MessageHelpers.receiveMessageFrom(tempConn);
            PingMessage castResp = (PingMessage) received;

            if (castResp.getStatus() == PingStatus.PING_RESPONSE)
                return this.replicaAddrsForClient.get(addr);
        } catch (Exception e) {
            e.printStackTrace();
            this.replicaAddrs.remove(addr);
        }

        return locateReplicaServer();
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
        if (request.getStatus() == DownloadStatus.DOWNLOAD_REQUEST) {
            // Check Auth Token
            // If not valid
            if (!this.checkAuthToken(request.getSender(), request.getAuthToken()).get("valid")) {
                MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(
                        DownloadStatus.DOWNLOAD_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null));
                return;
            }

            // Establishing connection to Primary File Server
            try (Socket primaryFileSocket = new Socket(this.primaryServerAddress.getAddress(),
                    this.primaryServerAddress.getPort());) {

                // Send a download request to the Primary File Server
                if (!MessageHelpers.sendMessageTo(primaryFileSocket, request)) {
                    MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(
                            DownloadStatus.DOWNLOAD_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null));
                    return;
                }

                // Getting the response from the Primary File Server
                Message response = MessageHelpers.receiveMessageFrom(primaryFileSocket);
                DownloadMessage castResponse = (DownloadMessage) response;
                response = null;

                if (castResponse.getStatus() != DownloadStatus.DOWNLOAD_REQUEST_VALID) {
                    // If Primary File Server returned failure
                    MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(
                            DownloadStatus.DOWNLOAD_REQUEST_INVALID, null, null, AuthServer.SERVER_NAME, null));
                    return;
                } else {
                    // If Primary File Server returned Success
                    HashMap<String, String> headers = new HashMap<String, String>();
                    InetSocketAddress replicaAddr = this.locateReplicaServer();

                    // If no Replica Servers exist anymore
                    if (replicaAddr == null) {
                        this.clientThreadPool.shutdown();
                        return;
                    }

                    headers.put("addr", replicaAddr.getHostString());
                    headers.put("port", Integer.toString(replicaAddr.getPort()));

                    MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(
                            DownloadStatus.DOWNLOAD_REQUEST_VALID, null, headers, AuthServer.SERVER_NAME, null));
                    return;
                }
            } catch (IOException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(
                        DownloadStatus.DOWNLOAD_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null));
                return;
            }

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_FAIL,
                    null, null, AuthServer.SERVER_NAME, null));
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
    private static synchronized void replicaSyncUpload(CopyOnWriteArrayList<InetSocketAddress> replicaAddrs,
            String code) {
        System.out.println(replicaAddrs.size());

        for (Iterator<InetSocketAddress> itr = replicaAddrs.iterator(); itr.hasNext();) {
            InetSocketAddress replicaAddr = itr.next();

            System.err.println("Creating Socket to " + replicaAddr);
            try (Socket replicaSocket = new Socket(replicaAddr.getHostName(), replicaAddr.getPort());) {
                System.err.println("Created Socket to " + replicaAddr);
                // Auth Server will wait around 10 minutes for a response
                replicaSocket.setSoTimeout((int) TimeUnit.MINUTES.toMillis(10));

                System.err.println("Sending Message to " + replicaAddr);
                // If message sending failed
                if (!MessageHelpers.sendMessageTo(replicaSocket,
                        new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_REQUEST, null, AuthServer.SERVER_NAME,
                                "tempAuthToken", code, null)))
                    throw new Exception();

                System.err.println("Receiving Message from " + replicaAddr);
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
                replicaAddrs.remove(replicaAddr);
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
            // Check Auth Token
            // If not valid
            if (!this.checkAuthToken(request.getSender(), request.getAuthToken()).get("valid")) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, AuthServer.SERVER_NAME, null, null));
                return;
            }

            // Establishing connection to Primary File Server
            try (Socket primaryFileSocket = new Socket(this.primaryServerAddress.getAddress(),
                    this.primaryServerAddress.getPort());) {
                // Send a upload request to the Primary File Server
                if (!MessageHelpers.sendMessageTo(primaryFileSocket, request)) {
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new UploadMessage(UploadStatus.UPLOAD_FAIL, null, AuthServer.SERVER_NAME, null, null));
                    return;
                }

                // Getting the response from the Primary File Server
                Message response = MessageHelpers.receiveMessageFrom(primaryFileSocket);
                UploadMessage castResponse = (UploadMessage) response;
                response = null;
                FileInfo fileInfo = castResponse.getFileInfo();

                if (castResponse.getStatus() != UploadStatus.UPLOAD_START) {
                    // If Primary File Server returned failure
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new UploadMessage(UploadStatus.UPLOAD_FAIL, null, AuthServer.SERVER_NAME, null, null));
                    return;
                } else {
                    // If Primary File Server returned success
                    // Sending UploadStart message to the client
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new UploadMessage(UploadStatus.UPLOAD_START, null, AuthServer.SERVER_NAME, null, null));

                    int buffSize = 1_048_576;
                    byte[] writeBuffer = new byte[buffSize];
                    BufferedInputStream fileFromClient = null;
                    BufferedOutputStream fileToServer = null;
                    try {
                        fileFromClient = new BufferedInputStream(this.clientSocket.getInputStream());
                        fileToServer = new BufferedOutputStream(primaryFileSocket.getOutputStream());

                        // Temporary var to keep track of total bytes read
                        long _temp_t = 0;
                        // Temporary var to keep track of read Bytes
                        int _temp_c = 0;
                        while ((_temp_t < fileInfo.getSize())
                                && ((_temp_c = fileFromClient.read(writeBuffer, 0, Math.min(writeBuffer.length,
                                        (int) Math.min(fileInfo.getSize(), Integer.MAX_VALUE)))) != -1)) {
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
                    System.err.println("Finished Uploading");

                    // Getting the response from the Primary File Server
                    response = MessageHelpers.receiveMessageFrom(primaryFileSocket);
                    castResponse = (UploadMessage) response;
                    response = null;
                    System.err.println("Got Succ Message");

                    if (castResponse.getStatus() != UploadStatus.UPLOAD_SUCCESS)
                        MessageHelpers.sendMessageTo(this.clientSocket, new UploadMessage(UploadStatus.UPLOAD_FAIL,
                                null, AuthServer.SERVER_NAME, "tempAuthToken", null));

                    System.err.println("Notifying Replicas");
                    // Notify each Replica Server to pull the newly uploaded File. If any Replica
                    // Server errors in this, remove it from the list
                    replicaSyncUpload(replicaAddrs, castResponse.getFileInfo().getCode());
                    System.err.println("Notified Replicas");

                    // Notify Client
                    MessageHelpers.sendMessageTo(this.clientSocket, new UploadMessage(UploadStatus.UPLOAD_SUCCESS, null,
                            AuthServer.SERVER_NAME, "tempAuthToken", castResponse.getFileInfo()));
                    System.err.println("Finished Notifying Client");

                }
            } catch (IOException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, AuthServer.SERVER_NAME, null, null));
                return;
            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new UploadMessage(UploadStatus.UPLOAD_REQUEST_INVALID, null, AuthServer.SERVER_NAME, null, null));
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
    private static synchronized void replicaSyncDelete(CopyOnWriteArrayList<InetSocketAddress> replicaAddrs,
            String code) {
        for (Iterator<InetSocketAddress> itr = replicaAddrs.iterator(); itr.hasNext();) {
            InetSocketAddress replicaAddr = itr.next();
            try (Socket replicaSocket = new Socket(replicaAddr.getAddress(), replicaAddr.getPort());) {

                // Auth Server will wait around 10 minutes for a response
                replicaSocket.setSoTimeout((int) TimeUnit.MINUTES.toMillis(10));

                // If message sending failed
                if (!MessageHelpers.sendMessageTo(replicaSocket, new SyncDeleteMessage(
                        SyncDeleteStatus.SYNCDELETE_REQUEST, null, AuthServer.SERVER_NAME, "tempAuthToken", code)))
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
                replicaAddrs.remove(replicaAddr);

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
            // Check Auth Token
            // If not valid
            HashMap<String, Boolean> authResp = this.checkAuthToken(request.getSender(), request.getAuthToken());
            if (!authResp.get("valid") && !authResp.get("isAdmin")) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_FAIL, null, null, "Auth Server", null, false));
                return;
            }

            // TODO: Check Auth Token
            // Establishing connection to Primary File Server
            try (Socket primaryFileSocket = new Socket(this.primaryServerAddress.getAddress(),
                    this.primaryServerAddress.getPort());) {
                // Forward delete request to the Primary File Server
                if (!MessageHelpers.sendMessageTo(primaryFileSocket,
                        new DeleteMessage(request.getStatus(), request.getCode(), request.getHeaders(),
                                request.getSender(), request.getAuthToken(), authResp.get("isAdmin")))) {
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new DeleteMessage(DeleteStatus.DELETE_FAIL, null, null, "Auth Server", null, false));
                    return;
                }

                // Getting the response from the Primary File Server
                Message response = MessageHelpers.receiveMessageFrom(primaryFileSocket);
                DeleteMessage castResponse = (DeleteMessage) response;
                response = null;

                if (castResponse.getStatus() != DeleteStatus.DELETE_SUCCESS) {
                    // If Primary File Server returned failure
                    MessageHelpers.sendMessageTo(this.clientSocket, new DeleteMessage(DeleteStatus.DELETE_FAIL, null,
                            null, AuthServer.SERVER_NAME, null, false));
                    return;
                } else {
                    // If Primary File Server returned success
                    MessageHelpers.sendMessageTo(this.clientSocket, new DeleteMessage(DeleteStatus.DELETE_SUCCESS, null,
                            null, AuthServer.SERVER_NAME, null, false));

                    // Syncing to Replicas
                    AuthServerHandler.replicaSyncDelete(this.replicaAddrs, request.getCode());
                }
            } catch (IOException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DeleteMessage(DeleteStatus.DELETE_FAIL, null, null, AuthServer.SERVER_NAME, null, false));
                return;
            }
        } else

        {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DeleteMessage(DeleteStatus.DELETE_INVALID, null, null, AuthServer.SERVER_NAME, null, false));
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
            // Check Auth Token
            // If not valid
            HashMap<String, Boolean> authResp = this.checkAuthToken(request.getSender(), request.getAuthToken());
            if (!authResp.get("valid") && !authResp.get("isAdmin")) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, AuthServer.SERVER_NAME, null));
                return;
            }

            // Establishing connection to Primary File Server
            try (Socket primaryFileSocket = new Socket(this.primaryServerAddress.getAddress(),
                    this.primaryServerAddress.getPort());) {

                // Send a getAllFileDetails request to the Primary File Server
                if (!MessageHelpers.sendMessageTo(primaryFileSocket, request)) {
                    MessageHelpers.sendMessageTo(this.clientSocket, new FileDetailsMessage(
                            FileDetailsStatus.FILEDETAILS_FAIL, null, AuthServer.SERVER_NAME, null));
                    return;
                }

                // Receiving response from the Primary File Server
                Message response = MessageHelpers.receiveMessageFrom(primaryFileSocket);
                FileDetailsMessage castResponse = (FileDetailsMessage) response;
                response = null;

                if (castResponse.getStatus() != FileDetailsStatus.FILEDETAILS_START) {
                    // If Primary File Server returned failure
                    MessageHelpers.sendMessageTo(this.clientSocket, new FileDetailsMessage(
                            FileDetailsStatus.FILEDETAILS_FAIL, null, AuthServer.SERVER_NAME, null));
                    return;
                }

                // If Primary File Server returned success
                // Sending a FileDetails Start message to the client
                MessageHelpers.sendMessageTo(this.clientSocket, new FileDetailsMessage(
                        FileDetailsStatus.FILEDETAILS_START, castResponse.getHeaders(), AuthServer.SERVER_NAME, null));

                int count = Integer.parseInt(castResponse.getHeaders().get("count"));
                ObjectOutputStream toClient = null;
                ObjectInputStream fromPrimaryServer = null;
                try {
                    toClient = new ObjectOutputStream(this.clientSocket.getOutputStream());
                    fromPrimaryServer = new ObjectInputStream(primaryFileSocket.getInputStream());

                    for (int i = 0; i < count; ++i) {
                        FileInfo temp = (FileInfo) fromPrimaryServer.readObject();
                        toClient.writeObject(temp);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                } finally {
                    toClient = null;
                    fromPrimaryServer = null;
                    primaryFileSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, AuthServer.SERVER_NAME, null));
                return;
            }
        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, AuthServer.SERVER_NAME, null));
            return;
        }
    }

    /**
     * Function that takes a MakeAdminMessage object from the Client, and tries to
     * grant Admin permissions to the specified User. If the specified User is
     * already an Admin, there is no effect.
     * 
     * @param request MakeAdminMessage object from the Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: MAKEADMIN_REQUEST
     * @sentInstructionIDs: MAKEADMIN_SUCCESS, MAKEADMIN_FAIL, MAKEADMIN_REQUESTFAIL
     */
    private void makeUserAdmin(MakeAdminMessage request) {
        if (request.getStatus() == MakeAdminStatus.MAKEADMIN_REQUEST) {
            // Check Auth Token
            // If not valid
            HashMap<String, Boolean> authResp = this.checkAuthToken(request.getSender(), request.getAuthToken());
            if (!authResp.get("valid") && !authResp.get("isAdmin")) {
                MessageHelpers.sendMessageTo(this.clientSocket, new MakeAdminMessage(
                        MakeAdminStatus.MAKEADMIN_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null, false));
                return;
            }

            try (PreparedStatement query = this.clientDB
                    .prepareStatement("UPDATE client SET Admin_Status = TRUE WHERE Username = ? ")) {

                query.setString(1, request.getNewAdmin());
                if (query.executeUpdate() == 1) {
                    MessageHelpers.sendMessageTo(this.clientSocket, new MakeAdminMessage(
                            MakeAdminStatus.MAKEADMIN_SUCCESS, null, null, AuthServer.SERVER_NAME, null, false));
                    this.clientDB.commit();
                } else {
                    MessageHelpers.sendMessageTo(this.clientSocket, new MakeAdminMessage(
                            MakeAdminStatus.MAKEADMIN_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null, false));
                    this.clientDB.rollback();
                }

            } catch (Exception e) {
                e.printStackTrace();
                try {
                    this.clientDB.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                MessageHelpers.sendMessageTo(this.clientSocket, new MakeAdminMessage(
                        MakeAdminStatus.MAKEADMIN_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null, false));
            }

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket, new MakeAdminMessage(MakeAdminStatus.MAKEADMIN_REQUEST_FAIL,
                    null, null, AuthServer.SERVER_NAME, null, false));
        }
    }

    /**
     * Function that takes a UnMakeAdminMessage object from the Client, and tries to
     * revoke Admin permissions from the specified User. If the specified User is
     * not an Admin, there is no effect.
     * 
     * @param request MakeAdminMessage object from the Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: UNMAKEADMIN_REQUEST
     * @sentInstructionIDs: UNMAKEADMIN_SUCCESS, UNMAKEADMIN_FAIL,
     *                      UNMAKEADMIN_REQUESTFAIL,
     */
    private void unMakeUserAdmin(UnMakeAdminMessage request) {
        if (request.getStatus() == UnMakeAdminStatus.UNMAKEADMIN_REQUEST) {
            // Check Auth Token
            // If not valid
            HashMap<String, Boolean> authResp = this.checkAuthToken(request.getSender(), request.getAuthToken());
            if (!authResp.get("valid") && !authResp.get("isAdmin")) {
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UnMakeAdminMessage(UnMakeAdminStatus.UNMAKEADMIN_REQUEST_INVALID, null, null,
                                AuthServer.SERVER_NAME, null, false));
                return;
            }

            try (PreparedStatement query = this.clientDB
                    .prepareStatement("UPDATE client SET Admin_Status = FALSE WHERE Username = ? ")) {
                query.setString(1, request.getOldAdmin());

                if (query.executeUpdate() == 1) {

                    MessageHelpers.sendMessageTo(this.clientSocket, new UnMakeAdminMessage(
                            UnMakeAdminStatus.UNMAKEADMIN_SUCCESS, null, null, AuthServer.SERVER_NAME, null, false));
                    this.clientDB.commit();
                } else {
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new UnMakeAdminMessage(UnMakeAdminStatus.UNMAKEADMIN_REQUEST_INVALID, null, null,
                                    AuthServer.SERVER_NAME, null, false));
                    this.clientDB.rollback();
                }

            } catch (Exception e) {
                e.printStackTrace();
                try {
                    this.clientDB.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                MessageHelpers.sendMessageTo(this.clientSocket, new UnMakeAdminMessage(
                        UnMakeAdminStatus.UNMAKEADMIN_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null, false));
            }

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket, new UnMakeAdminMessage(
                    UnMakeAdminStatus.UNMAKEADMIN_REQUEST_FAIL, null, null, AuthServer.SERVER_NAME, null, false));
        }
    }
}