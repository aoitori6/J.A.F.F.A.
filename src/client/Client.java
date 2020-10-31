package client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

import message.*;
import misc.FileInfo;
import statuscodes.*;

public class Client {
    protected boolean isAdmin;

    protected String name;
    protected Socket fileSocket;
    protected String authToken;

    protected InetSocketAddress authAddr;

    public Client(InetSocketAddress authAddr) {
        this.authAddr = authAddr;
    }

    /**
     * @param name Client Username
     * @param pass Client Password
     * 
     * 
     *             <p>
     *             Message Specs
     * @sentInstructionIDs: REGISTER_REQUEST
     * @expectedInstructionIDs: REGISTER_SUCCESS, REGISTER_FAIL
     */

    public boolean register(String name, String pass) {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Sending Register Message
            if (!MessageHelpers.sendMessageTo(authSocket,
                    new RegisterMessage(RegisterStatus.REGISTER_REQUEST, pass, null, name)))
                return false;
            
            // Reading AuthServer's response
            Message response = MessageHelpers.receiveMessageFrom(authSocket);
            RegisterMessage castResponse = (RegisterMessage) response;
            response = null;

            // Parsing AuthServer's response
            if (castResponse.getStatus() != RegisterStatus.REGISTER_SUCCESS)
                return false;

            // Logging in user
            if (logIn(name, pass, false) == false)
                System.err.println("Critical ERROR. User registered but couldn't login!");

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * @param name Client Username
     * @param pass Client Password
     * 
     * 
     *             <p>
     *             Message Specs
     * @sentInstructionIDs: LOGIN_REQUEST
     * @expectedInstructionIDs: LOGIN_SUCCESS, LOGIN_FAIL
     * @sentHeaders: pass:password
     * @expectedHeaders: authToken:authToken (null if LOGIN_FAIL)
     */

    public boolean logIn(String name, String pass, boolean isAdmin) {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Sending login Message
            if (!MessageHelpers.sendMessageTo(authSocket,
                    new LoginMessage(LoginStatus.LOGIN_REQUEST, pass, isAdmin, authToken, null, name)))
                return false;

            // Reading AuthServer's response
            Message response = MessageHelpers.receiveMessageFrom(authSocket);
            LoginMessage castResponse = (LoginMessage) response;
            response = null;

            // Parsing AuthServer's response
            if (castResponse.getStatus() != LoginStatus.LOGIN_SUCCESS)
                return false;

            // Setting authToken if true
            this.authToken = castResponse.getAuthToken();
            this.name = name;
            this.isAdmin = castResponse.getIfAdmin();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 
     * <p>
     * Message Specs
     * 
     * @sentInstructionIDs: LOGOUT_REQUEST
     * @expectedInstructionIDs: LOGOUT_SUCCESS, LOGOUT_FAIL
     */

    public boolean logout() {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Sending Logout Message to AuthServer
            if (!MessageHelpers.sendMessageTo(authSocket,
                    new LogoutMessage(LogoutStatus.LOGOUT_REQUEST, this.authToken, null, this.name)))
                return false;

            // Reading AuthServer's response
            Message response = MessageHelpers.receiveMessageFrom(authSocket);
            LogoutMessage castResponse = (LogoutMessage) response;
            response = null;

            // Parsing AuthServer's response
            if (castResponse.getStatus() != LogoutStatus.LOGOUT_SUCCESS)
                return false;
            else
                return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @param code     File Code
     * @param savePath Path to save file to
     * 
     * 
     *                 <p>
     *                 Message Specs
     * @sentInstructionIDs: DOWNLOAD_REQUEST
     * @expectedInstructionIDs: DOWNLOAD_SUCCESS, DOWNLOAD_FAIL
     * @expectedHeaders: fileName:FileName, size:FileSize
     */
    public DownloadStatus downloadFile(String code, Path savePath) {
        HashMap<String, String> fileServerAddress;
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Sending a DownloadRequest to the AuthServer
            if (!MessageHelpers.sendMessageTo(authSocket,
                    new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST, code, null, this.name, this.authToken)))
                return DownloadStatus.DOWNLOAD_REQUEST_FAIL;

            // Reading AuthServer's Response
            Message authServerResponse = MessageHelpers.receiveMessageFrom(authSocket);
            DownloadMessage authServerCastResponse = (DownloadMessage) authServerResponse;
            authServerResponse = null;

            if (authServerCastResponse.getStatus() != DownloadStatus.DOWNLOAD_REQUEST_VALID)
                return DownloadStatus.DOWNLOAD_REQUEST_INVALID;

            // If DownloadRequest was valid, get the ReplicaServers address and port
            fileServerAddress = authServerCastResponse.getHeaders();

        } catch (Exception e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        }

        // Attempt to connect to FileServer
        try {
            this.fileSocket = new Socket(fileServerAddress.get("addr"),
                    Integer.parseInt(fileServerAddress.get("port")));
        } catch (Exception e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        }

        // Send a DownloadRequest to the File Server
        // Expect DOWNLOAD_START if file exists and all is successful
        if (!MessageHelpers.sendMessageTo(fileSocket,
                new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST, code, null, name, this.authToken)))
            return DownloadStatus.DOWNLOAD_FAIL;

        // Parse Response
        Message response = MessageHelpers.receiveMessageFrom(fileSocket);
        DownloadMessage castResponse = (DownloadMessage) response;
        response = null;

        // If File doesn't exist
        if (castResponse.getStatus() != DownloadStatus.DOWNLOAD_START)
            return DownloadStatus.DOWNLOAD_FAIL;

        HashMap<String, String> responseHeaders = castResponse.getHeaders();

        // Check if Save Path exists
        // If not, try to create it
        savePath = savePath.resolve(responseHeaders.get("fileName"));

        if (Files.notExists(savePath))
            try {
                Files.createFile(savePath);
            } catch (IOException e) {
                e.printStackTrace();
            }

        // Start downloading the file
        // TODO: Set and fine tune buffer size
        // Temporary Buffer Size in Bytes
        int buffSize = 1_048_576;
        byte[] writeBuffer = new byte[buffSize];
        BufferedInputStream fileFromServer;
        BufferedOutputStream fileOnClient;
        System.err.print("LOG: Beginning File Download");
        try {
            // Begin connecting to file Server and establish read/write Streams
            fileFromServer = new BufferedInputStream(fileSocket.getInputStream());
            fileOnClient = new BufferedOutputStream(new FileOutputStream(savePath.toString()));

            // Temporary var to keep track of total bytes read
            long _temp_t = 0;
            // Temporary var to keep track of read Bytes
            int _temp_c = 0;
            long fileSize = Long.parseLong(responseHeaders.get("fileSize"));
            while ((_temp_t < fileSize) && ((_temp_c = fileFromServer.read(writeBuffer, 0,
                    Math.min(writeBuffer.length, (int) Math.min(fileSize - _temp_t, Integer.MAX_VALUE)))) != -1)) {

                fileOnClient.write(writeBuffer, 0, _temp_c);
                fileOnClient.flush();
                _temp_t += _temp_c;
            }

            // File successfully downloaded
            System.err.print("LOG: Finishing File Download");

            fileOnClient.close();
            return DownloadStatus.DOWNLOAD_SUCCESS;

        } catch (

        IOException e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        } finally {
            writeBuffer = null;
            fileFromServer = null;
            fileOnClient = null;
        }
    }

    /**
     * @param filePath Path To The File To Upload
     * 
     *                 <p>
     *                 Message Specs
     * @sentInstructionIDs: UPLOAD_REQUEST
     * @expectedInstructionIDs: UPLOAD_START, UPLOAD_SUCCESS, UPLOAD_FAIL
     */

    public String uploadFile(Path filePath, Integer downloadCap, String timestamp) {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Send a UploadRequest to the File Server
            // Expect UPLOAD_START if all is successful
            if (!MessageHelpers.sendMessageTo(authSocket,
                    new UploadMessage(UploadStatus.UPLOAD_REQUEST, null, this.name, this.authToken,
                            new FileInfo(filePath.getFileName().toString(), null, filePath.toFile().length(), this.name,
                                    downloadCap, timestamp))))
                return null;

            // Parse Response
            Message response = MessageHelpers.receiveMessageFrom(authSocket);
            UploadMessage castResponse = (UploadMessage) response;
            response = null;

            if (castResponse.getStatus() != UploadStatus.UPLOAD_START)
                return null;

            // Start Uploading the file
            // TODO: Set and fine tune buffer size
            // Temporary Buffer Size in Bytes

            int buffSize = 1_048_576;
            byte[] writeBuffer = new byte[buffSize];
            BufferedInputStream fileOnClient = null;
            BufferedOutputStream fileToServer = null;
            System.err.println("LOG: Beginning File Upload");
            try {
                // Begin connecting to file Server and establish read/write Streams
                fileOnClient = new BufferedInputStream(new FileInputStream(filePath.toString()));
                fileToServer = new BufferedOutputStream(authSocket.getOutputStream());

                System.err.println(filePath.toFile().length());
                System.err.println(filePath.toFile().length());

                // Temporary var to keep track of total bytes read
                long _temp_t = 0;
                // Temporary var to keep track of read Bytes
                int _temp_c = 0;
                while ((_temp_t < filePath.toFile().length())
                        && ((_temp_c = fileOnClient.read(writeBuffer, 0, Math.min(writeBuffer.length,
                                (int) Math.min(filePath.toFile().length(), Integer.MAX_VALUE)))) != -1)) {

                    fileToServer.write(writeBuffer, 0, _temp_c);
                    fileToServer.flush();
                    _temp_t += _temp_c;
                }

                // File successfully uploaded
                System.err.println("LOG: Finishing File Upload");

            } catch (IOException e) {
                e.printStackTrace();
                return null;
            } finally {
                writeBuffer = null;
                try {
                    fileOnClient.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Parse Response
            response = MessageHelpers.receiveMessageFrom(authSocket);
            castResponse = (UploadMessage) response;
            response = null;

            if (castResponse.getFileInfo() == null)
                return null;
            // Returning the Code if Everthing was successful
            return castResponse.getFileInfo().getCode();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * @param code    File Code
     * @param isAdmin Is User Admin or Not
     * 
     *                <p>
     *                Message Specs
     * @sentInstructionIDs: DELETE_REQUEST
     * @expectedInstructionIDs: DELETE_SUCCESS, DELETE_FAIL, DELETE_INVALID
     */

    public boolean deleteFile(String code, boolean isAdmin) {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Send a DeleteRequest to the File Server
            if (!MessageHelpers.sendMessageTo(authSocket,
                    new DeleteMessage(DeleteStatus.DELETE_REQUEST, code, null, this.name, this.authToken, isAdmin)))
                return false;

            // Response From the Server
            Message response = MessageHelpers.receiveMessageFrom(authSocket);
            DeleteMessage castResponse = (DeleteMessage) response;
            response = null;

            if (castResponse.getStatus() != DeleteStatus.DELETE_SUCCESS)
                return false;
            else
                return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @return A list of all available File Information from the File DB (if admin)
     *         or only those Files uploaded by a User
     * 
     * 
     *         <p>
     *         Message Specs
     * @sentInstructionIDs: FILEDETAILS_REQUEST
     * @expectedInstructionIDs: FILEDETAILS_SUCCESS, FILEDETAILS_FAIL
     * @expectedHeaders: count:FileCount, timestamp:ServerTimestamp (at which time
     *                   data was feteched)
     */
    public ArrayList<FileInfo> getAllFileData() {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Send a FileDetailsRequest to the Auth Server
            // Expect FILEDETAILS_START if file exists and all is successful
            if (!MessageHelpers.sendMessageTo(authSocket, new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_REQUEST,
                    null, this.name, this.authToken, this.isAdmin)))
                return null;

            // Parse Response
            Message response = MessageHelpers.receiveMessageFrom(authSocket);
            FileDetailsMessage castResponse = (FileDetailsMessage) response;
            response = null;

            if (castResponse.getStatus() != FileDetailsStatus.FILEDETAILS_START)
                return null;

            // Prepare to receive File Details
            ArrayList<FileInfo> currFileDetails = null;
            ObjectInputStream fromServer = null;
            try {
                int size = Integer.parseInt(castResponse.getHeaders().get("count"));
                currFileDetails = new ArrayList<FileInfo>(size);
                fromServer = new ObjectInputStream(authSocket.getInputStream());

                for (int i = 0; i < size; ++i) {
                    FileInfo temp = (FileInfo) fromServer.readObject();
                    currFileDetails.add(temp);
                }

                return currFileDetails;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
