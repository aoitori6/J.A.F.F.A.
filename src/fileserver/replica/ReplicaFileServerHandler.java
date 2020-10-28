package fileserver.replica;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import misc.FileInfo;
import message.*;
import statuscodes.DownloadStatus;
import statuscodes.FileDetailsStatus;
import statuscodes.SyncDeleteStatus;
import statuscodes.SyncUploadStatus;

final public class ReplicaFileServerHandler implements Runnable {
    private final Socket clientSocket;
    private final Connection fileDB;
    private final Socket primaryFileServerSocket;
    private final Socket authServer;

    private final static String HOME = System.getProperty("user.home");

    ReplicaFileServerHandler(Socket clientSocket, Connection fileDB, Socket primaryFileSeverSocket, Socket authServer) {
        this.clientSocket = clientSocket;
        this.fileDB = fileDB;
        this.primaryFileServerSocket = primaryFileSeverSocket;
        this.authServer = authServer;
    }

    @Override
    /**
     * This method contains the central logic of the Replica File Server. It listens
     * for {@code Message} objects from the Client and Auth Server, and handles them
     * as required by looking at the status field.
     */
    public void run() {
        // Expect a Message from the Client or Auth Server
        Message request = MessageHelpers.receiveMessageFrom(this.clientSocket);

        // Central Logic
        // Execute different methods after checking Message status

        switch (request.getRequestKind()) {
            case Download:
                serverDownload((DownloadMessage) request);
                break;
            case Upload:
                serverUpload((SyncUploadMessage) request);
                break;
            case Delete:
                deleteFile((SyncDeleteMessage) request);
                break;
            case FileDetails:
                getAllFileData((FileDetailsMessage) request);
                break;
            default:
                break;

        }

        try {
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes a DownloadMessage object from the Client and queries the associated
     * File Database if the supplied file code exists. If the code exists, File is
     * sent to the Client and a Success message is sent to the Primary File Server,
     * otherwise an error Message is returned.
     * 
     * @param request Download Request received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DOWNLOAD_REQUEST
     * @sentInstructionIDs: DOWNLOAD_START, DOWNLOAD_SUCCESS, DOWNLOAD_FAIL
     * @expectedHeaders: code:code
     * @sentHeaders: fileName:FileName
     */
    private void serverDownload(DownloadMessage request) {
        // TODO: Re-checking auth token
        HashMap<String, String> headers = new HashMap<String, String>();
        if (request.getStatus() == DownloadStatus.DOWNLOAD_REQUEST) {

            // First check fileDB if Code exists
            String query = "SELECT * FROM FILE WHERE Code = ?;";
            PreparedStatement checkCode;
            ResultSet queryResp;
            try {
                // Querying DB
                checkCode = fileDB.prepareStatement(query);
                checkCode.setString(1, request.getHeaders().get("code"));
                queryResp = checkCode.executeQuery();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                query = null;
                checkCode = null;
            }

            // Processing Result Set
            // First check if Result Set is empty
            File filePath;
            try {
                String code;
                headers.clear();
                if (queryResp.next() == false) {
                    // Implies Code doesn't exist
                    headers.put("fileName", null);
                    // Sending failure message to client
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new DownloadMessage(DownloadStatus.DOWNLOAD_FAIL, headers, "File Server", "tempServerKey"));
                    // Sending failure message to Primary File Server
                    MessageHelpers.sendMessageTo(this.primaryFileServerSocket, new DownloadMessage(
                            DownloadStatus.DOWNLOAD_FAIL, null, "Replica File Server", "tempServerKey"));
                    return;
                }

                code = queryResp.getString("code");
                // Getting path of File on the machine
                // General Path expected is USER_HOME/sharenowdb/fileCode/file
                // TODO: Make DB path mutable
                filePath = Paths.get(HOME, "sharenowdb", code).toFile().listFiles()[0];

                // Check if file exists, should always be the case
                if (!filePath.exists()) {
                    System.out
                            .println("ERROR. Critical error in File DB! File exists in MySQL DB but Path was invalid!");
                    this.clientSocket.close();
                    MessageHelpers.sendMessageTo(this.primaryFileServerSocket, new DownloadMessage(
                            DownloadStatus.DOWNLOAD_FAIL, null, "Replica File Server", "tempServerKey"));
                    return;
                }

                headers.put("fileName", filePath.getName());
                MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(DownloadStatus.DOWNLOAD_START,
                        headers, "Replica File Server", "tempServerKey"));

            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                headers = null;
            }

            // Prep for File transfer
            int buffSize = 1_048_576;
            byte[] writeBuffer = new byte[buffSize];
            BufferedInputStream fileFromDB = null;
            BufferedOutputStream fileToClient = null;

            try {
                // Begin connecting to file Server and establish read/write Streams
                fileFromDB = new BufferedInputStream(new FileInputStream(filePath.getAbsolutePath()));
                fileToClient = new BufferedOutputStream(clientSocket.getOutputStream());

                // Temporary var to keep track of read Bytes
                int _temp_c;
                while ((_temp_c = fileFromDB.read(writeBuffer, 0, writeBuffer.length)) != -1) {
                    fileToClient.write(writeBuffer, 0, _temp_c);
                    fileToClient.flush();
                }

                // File successfully downloaded
                fileFromDB.close();

                // Send a success message to the Primary File Server
                MessageHelpers.sendMessageTo(this.primaryFileServerSocket, new DownloadMessage(
                        DownloadStatus.DOWNLOAD_SUCCESS, null, "Replica File Sever", "tempServerKey"));
                return;

            } catch (Exception e) {
                e.printStackTrace();

                // File failed to download, sending failure message to the Primary File Server
                MessageHelpers.sendMessageTo(this.primaryFileServerSocket, new DownloadMessage(
                        DownloadStatus.DOWNLOAD_FAIL, null, "Replica File Server", "tempServerKey"));
                return;
            } finally {
                writeBuffer = null;
                try {
                    fileFromDB.close();
                    fileToClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        else {
            headers.put("fileName", null);
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DownloadMessage(DownloadStatus.DOWNLOAD_FAIL, headers, "Replica File Server", "tempServerKey"));
            MessageHelpers.sendMessageTo(this.primaryFileServerSocket,
                    new DownloadMessage(DownloadStatus.DOWNLOAD_FAIL, null, "Replica File Server", "tempServerKey"));
        }
        headers = null;
    }

    /**
     * Takes a SyncUploadMessage object from the Auth Server and sends a request to
     * the Primary File Server to recieve the file.
     * 
     * @param request SyncUploadMessage received from Auth Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: SYNCUPLOAD_REQUEST
     * @sentInstructionIDs: SYNCUPLOAD_START, SYNCUPLOAD_SUCCESS, SYNCUPLOAD_FAIL
     */
    private void serverUpload(SyncUploadMessage request) {
        // TODO: Re-checking auth token
        if (request.getStatus() == SyncUploadStatus.SYNCUPLOAD_REQUEST) {

            // Send a request to the Primary File Server
            if (!MessageHelpers.sendMessageTo(this.primaryFileServerSocket, request)) {
                MessageHelpers.sendMessageTo(this.authServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL,
                        null, "Replica File Server", "tempServerKey", null, null));
                return;
            }

            // Receive a response from the Primary File Server
            Message response = MessageHelpers.receiveMessageFrom(this.primaryFileServerSocket);
            SyncUploadMessage castResponse = (SyncUploadMessage) response;
            response = null;

            // If Primary File Sever returned failure
            if (castResponse.getStatus() != SyncUploadStatus.SYNCUPLOAD_START) {
                MessageHelpers.sendMessageTo(this.authServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL,
                        null, "Replica File Server", "tempServerKey", null, null));
                return;
            } else {
                // Primary File Sever returned success
                // First generate a Path to the upload folder
                Path fileFolder;
                String code;
                try {
                    // Get the code sent by the Auth Server
                    code = request.getFileCode();
                    fileFolder = Paths.get(HOME, "sharenowdb", code);
                    // Create the file itself in the Folder
                    fileFolder = Files.createFile(fileFolder.resolve(castResponse.getFileInfo().getName()));
                } catch (IOException e1) {
                    e1.printStackTrace();
                    MessageHelpers.sendMessageTo(this.authServer,
                            new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null, "Replica File Server",
                                    "tempServerKey", null, null));
                    return;
                }

                // Prep for File transfer
                int buffSize = 1_048_576;
                byte[] readBuffer = new byte[buffSize];
                BufferedInputStream fileFromPrimaryFileServer = null;

                try (BufferedOutputStream fileToDB = new BufferedOutputStream(
                        new FileOutputStream(fileFolder.toString()));) {
                    // Begin connecting to file Server and establish read/write Streams
                    fileFromPrimaryFileServer = new BufferedInputStream(this.primaryFileServerSocket.getInputStream());

                    // Temporary var to keep track of total bytes read
                    int _temp_t = 0;
                    // Temporary var to keep track of bytes read on each iteration
                    int _temp_c;
                    while (((_temp_c = fileFromPrimaryFileServer.read(readBuffer, 0, readBuffer.length)) != -1)
                            || (_temp_t <= castResponse.getFileInfo().getSize())) {
                        fileToDB.write(readBuffer, 0, _temp_c);
                        fileToDB.flush();
                        _temp_t += _temp_c;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    MessageHelpers.sendMessageTo(this.authServer,
                            new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null, "Replica File Server",
                                    "tempServerKey", null, null));
                    return;
                } finally {
                    readBuffer = null;
                    fileFromPrimaryFileServer = null;
                }

                // If file was successfully uploaded, add an entry to the File DB
                try (PreparedStatement query = this.fileDB
                        .prepareStatement("INSERT INTO file(code, uploader, filename) VALUES(?,?,?)");) {
                    query.setString(1, code);
                    query.setString(2, request.getSender());
                    query.setString(3, fileFolder.toFile().getName());
                    query.executeUpdate();
                } catch (Exception e) {
                    e.printStackTrace();
                    MessageHelpers.sendMessageTo(this.authServer,
                            new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null, "Replica File Server",
                                    "tempServerKey", null, null));
                    return;
                }

                // Everything was successful, send a success message to authSever
                MessageHelpers.sendMessageTo(this.authServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_SUCCESS,
                        null, "Replica File Server", "tempServerKey", null, null));
                return;
            }
        } else {
            MessageHelpers.sendMessageTo(this.authServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null,
                    "Replica File Server", "tempServerKey", null, null));
            return;
        }

    }

    /**
     * Helper function for deleteFile. Queries the associated File DB to see if the
     * supplied File Code exists, it marks the file for deletion in the DB. This 
     * method serializes access to the File DB, and thus ensures that a given File 
     * is only deleted once.
     * 
     * @param fileDB  File Database to check against
     * @param code    File Code to be deleted
     */
    private static synchronized boolean deleteFromDB(Connection fileDB, String code) {
        PreparedStatement update;
        try {
            // Preparing Statement
            update = fileDB.prepareStatement("UPDATE file SET deletable = TRUE WHERE code = ?");
            update.setString(1, code);

            // Executing Query and checking responses
            if (update.executeUpdate() == 1)
                return true;
            else
                return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Takes a SyncDeleteMesage object from the Auth Server and queries the DB to see if the
     * File exists. If the query is successful, the File is deleted from both the File DB
     * and the associated File System and the Auth Server is notified. If the query fails,
     * then the Auth Server is notified about the reason for the same.
     * 
     * <p>
     * This method does not cancel any active downloads on the file to be deleted.
     * It only guarantees that the file will be marked as Deletable, and it is up to
     * other methods to respect that field until actual File deletion occurs. Files
     * present on the File System but not in the DB will be scheduled for deletion
     * by another listener.
     * 
     * @param request DeleteMessage received from Auth Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DELETE_REQUEST
     * @sentInstructionIDs: DELETE_SUCCESS, DELETE_FAIL, DELETE_INVALID
     */

    private void deleteFile(SyncDeleteMessage request) {
        if (request.getStatus() == SyncDeleteStatus.SYNCDELETE_REQUEST) {

            // Attempt deletion from the File DB
            if (deleteFromDB(this.fileDB, request.getFileCode()))
                MessageHelpers.sendMessageTo(this.authServer, new SyncDeleteMessage(SyncDeleteStatus.SYNCDELETE_SUCCESS,
                        null, null, "Replica File Server", "tempServerKey"));
            else {
                MessageHelpers.sendMessageTo(this.authServer, new SyncDeleteMessage(SyncDeleteStatus.SYNCDELETE_FAIL,
                        null, null, "Replica File Server", "tempServerKey"));
            }

        } else {
            MessageHelpers.sendMessageTo(this.authServer, new SyncDeleteMessage(SyncDeleteStatus.SYNCDELETE_FAIL, null,
                    null, "Replica File Server", "tempServerKey"));
        }
    }

    /**
     * Takes a FileDetailsMessage object from the Client and first verifies if the
     * Client is authorized. If so, it queries the associated File DB for a list of
     * files and their details, and sends the same to the Client. A timestamp is
     * sent along with the details, and the results are accurrate to the timestamp.
     * 
     * @param request FileDetailsMessage received from the Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: FILEDETAILS_REQUEST
     * @sentInstructionIDs: FILEDETAILS_SUCCESS, FILEDETAILS_FAIL
     * @sentHeaders: count:FileCount, timestamp:ServerTimestamp (at which time data
     *               was feteched)
     */
    private void getAllFileData(FileDetailsMessage request) {
        if (request.getStatus() != FileDetailsStatus.FILEDETAILS_REQUEST) {
            // TODO: Check authCode vs Auth DB to ensure Client is an Admin

            ArrayList<FileInfo> currFileInfo = new ArrayList<FileInfo>(0);

            // Querying associated File DB
            try (ResultSet queryResp = fileDB.createStatement()
                    .executeQuery("SELECT * FROM files WHERE deletable = FALSE");) {

                // Parsing Result
                while (queryResp.next()) {
                    currFileInfo.add(new FileInfo(queryResp.getString("filename"), queryResp.getString("code"),
                            Paths.get(HOME, "sharenowdb", queryResp.getString("code")).toFile().length(),
                            queryResp.getString("uploader"),
                            Integer.parseInt(queryResp.getString("downloads_remaining")),
                            queryResp.getString("deletion_timestamp")));
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }

            try (ObjectOutputStream toClient = new ObjectOutputStream(this.clientSocket.getOutputStream());) {
                // Sending Start message to Client
                HashMap<String, String> headers = new HashMap<String, String>();
                headers.put("code", String.valueOf(currFileInfo.size()));
                headers.put("timestamp", new Date().toString());
                MessageHelpers.sendMessageTo(this.clientSocket, new FileDetailsMessage(
                        FileDetailsStatus.FILEDETAILS_START, headers, "File Server", "tempAuthToken"));
                headers = null;

                // Beginning transfer of details
                for (FileInfo temp : currFileInfo)
                    toClient.writeObject(temp);

            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                currFileInfo = null;
            }

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, "File Server", "tempServerKey"));
        }
    }

}
