package server.fileserver.replica;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.stream.Stream;

import misc.FileInfo;
import signals.base.Message;
import signals.derived.download.*;
import signals.derived.ping.*;
import signals.derived.syncdelete.*;
import signals.derived.syncupload.*;
import signals.utils.MessageHelpers;

final class ReplicaFileServerHandler implements Runnable {
    private final Socket clientSocket;

    ReplicaFileServerHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
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
            case SyncUpload:
                serverUpload((SyncUploadMessage) request);
                break;
            case SyncDelete:
                deleteFile((SyncDeleteMessage) request);
                break;
            case Ping:
                pingResponse((PingMessage) request);
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
     * Takes a PingMessage request from the Cline and responds with an empty
     * message. Used to test if the server is still alive or not.
     * 
     * @param request PingMessage received from the Client
     */
    private void pingResponse(PingMessage request) {
        MessageHelpers.sendMessageTo(this.clientSocket,
                new PingMessage(PingStatus.PING_RESPONSE, null, ReplicaFileServer.SERVER_NAME));

    }

    /**
     * Takes a DownloadMessage object from the Client and queries the associated
     * File Database if the supplied file code exists. If the code exists, File is
     * sent to the Client. The Primary Server is notified about whether the download
     * was successful or not.
     * 
     * @param request Download Request received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DOWNLOAD_REQUEST
     * @sentInstructionIDs: DOWNLOAD_START, DOWNLOAD_SUCCESS, DOWNLOAD_FAIL
     * @sentHeaders: fileName:FileName, fileSize:FileSize
     */
    private void serverDownload(DownloadMessage request) {
        // First check the File System if Code exists
        File filePath = ReplicaFileServer.FILESTORAGEFOLDER_PATH.resolve(request.getCode()).toFile().listFiles()[0];
        if (!filePath.exists()) {
            MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_INVALID,
                    request.getCode(), null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN));
            return;
        }

        // If it does
        if (request.getStatus() == DownloadStatus.DOWNLOAD_REQUEST) {

            boolean downloadSuccess = false;
            HashMap<String, String> headers = new HashMap<String, String>(2);
            headers.put("fileSize", Long.toString(filePath.length()));
            headers.put("fileName", filePath.getName());

            MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(DownloadStatus.DOWNLOAD_START, null,
                    headers, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN));

            // Prep for File transfer
            int buffSize = 1_048_576;
            byte[] writeBuffer = new byte[buffSize];

            // Begin connecting to file Server and establish read/write Streams
            try (BufferedInputStream fileFromDB = new BufferedInputStream(
                    new FileInputStream(filePath.getAbsolutePath()));

                    BufferedOutputStream fileToClient = new BufferedOutputStream(
                            this.clientSocket.getOutputStream());) {

                // Temporary var to keep track of total bytes read
                long _temp_t = 0;
                // Temporary var to keep track of bytes read on each iteration
                int _temp_c = 0;
                while ((_temp_t < filePath.length())
                        && ((_temp_c = fileFromDB.read(writeBuffer, 0, Math.min(writeBuffer.length,
                                (int) Math.min(filePath.length() - _temp_t, Integer.MAX_VALUE)))) != -1)) {

                    fileToClient.write(writeBuffer, 0, _temp_c);
                    fileToClient.flush();
                    _temp_t += _temp_c;
                }

                // File successfully downloaded
                downloadSuccess = true;
            } catch (Exception e) {
                // File failed to download
                e.printStackTrace();
            }

            // Notifying Primary Server about success state of download
            try (Socket primarySocket = new Socket(ReplicaFileServer.primaryServerAddr.getAddress(),
                    ReplicaFileServer.primaryServerAddr.getPort());) {
                // Successful
                if (downloadSuccess) {
                    MessageHelpers.sendMessageTo(primarySocket, new DownloadMessage(DownloadStatus.DOWNLOAD_SUCCESS,
                            request.getCode(), null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN));
                }

                // Unsuccessful
                else {
                    MessageHelpers.sendMessageTo(primarySocket, new DownloadMessage(DownloadStatus.DOWNLOAD_FAIL,
                            request.getCode(), null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        else {
            MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_INVALID,
                    null, null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN));
        }
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
        if (request.getStatus() == SyncUploadStatus.SYNCUPLOAD_REQUEST) {

            FileInfo fileInfo;
            try (Socket primarySocket = new Socket(ReplicaFileServer.primaryServerAddr.getAddress(),
                    ReplicaFileServer.primaryServerAddr.getPort());) {

                System.err.println("Sending Message to Primary Server");
                // Send a request to the Primary File Server
                if (!MessageHelpers.sendMessageTo(primarySocket, request)) {
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null, ReplicaFileServer.SERVER_NAME,
                                    ReplicaFileServer.SERVER_TOKEN, null, null));
                    return;
                }

                System.err.println("Received Message from Primary Server");
                // Receive a response from the Primary File Server
                Message response = MessageHelpers.receiveMessageFrom(primarySocket);
                SyncUploadMessage castResponse = (SyncUploadMessage) response;
                response = null;

                // If Primary File Sever returned failure
                if (castResponse.getStatus() != SyncUploadStatus.SYNCUPLOAD_START) {
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null, ReplicaFileServer.SERVER_NAME,
                                    ReplicaFileServer.SERVER_TOKEN, null, null));
                    return;
                }

                // Primary File Sever returned success
                // First generate a Path to the upload folder
                Path filePath;
                try {
                    // Get the code sent by the Auth Server
                    fileInfo = castResponse.getFileInfo();
                    filePath = ReplicaFileServer.FILESTORAGEFOLDER_PATH.resolve(fileInfo.getCode());
                    // Create the folders
                    Files.createDirectories(filePath);
                    // Create the File itself in the folder
                    filePath = Files.createFile(filePath.resolve(castResponse.getFileInfo().getName()));
                } catch (IOException e1) {
                    e1.printStackTrace();
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null, ReplicaFileServer.SERVER_NAME,
                                    ReplicaFileServer.SERVER_TOKEN, null, null));
                    return;
                }

                System.err.println("Preparing for file from Primary Server");
                // Prep for File transfer
                int buffSize = 1_048_576;
                byte[] readBuffer = new byte[buffSize];
                BufferedInputStream fileFromPrimaryFileServer = null;

                try (BufferedOutputStream fileToDB = new BufferedOutputStream(
                        new FileOutputStream(filePath.toString()));) {
                    // Begin connecting to Primary Server and establish read/write Streams
                    fileFromPrimaryFileServer = new BufferedInputStream(primarySocket.getInputStream());

                    // Temporary var to keep track of total bytes read
                    long _temp_t = 0;
                    // Temporary var to keep track of bytes read on each iteration
                    int _temp_c = 0;
                    System.err.println(fileInfo.getSize());
                    while (_temp_t < fileInfo.getSize()) {

                        _temp_c = fileFromPrimaryFileServer.read(readBuffer, 0, Math.min(readBuffer.length,
                                (int) Math.min(fileInfo.getSize() - _temp_t, Integer.MAX_VALUE)));

                        System.err.println("READ: " + _temp_c);
                        fileToDB.write(readBuffer, 0, _temp_c);
                        fileToDB.flush();
                        _temp_t += _temp_c;
                        System.err.println("TOTAL: " + _temp_t);
                        System.err.println(fileFromPrimaryFileServer.available());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    MessageHelpers.sendMessageTo(primarySocket, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL,
                            null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN, null, null));
                    return;
                } finally {
                    readBuffer = null;
                    fileFromPrimaryFileServer = null;
                }

                System.err.println("File Transfer Done from Primary Server");

                // If file was successfully uploaded, add an entry to the File DB
                try (PreparedStatement query = ReplicaFileServer.fileDB
                        .prepareStatement("INSERT INTO replica_file(code, uploader, filename) VALUES(?,?,?)");) {
                    query.setString(1, fileInfo.getCode());
                    query.setString(2, fileInfo.getUploader());
                    query.setString(3, fileInfo.getName());
                    query.executeUpdate();
                    ReplicaFileServer.fileDB.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL, null, ReplicaFileServer.SERVER_NAME,
                                    ReplicaFileServer.SERVER_TOKEN, null, null));
                    return;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            System.err.println("Sending Message to Auth");
            // Everything was successful, send a success message to authSever
            MessageHelpers.sendMessageTo(this.clientSocket, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_SUCCESS,
                    null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN, null, null));
            return;

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL,
                    null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN, null, null));
            return;
        }

    }

    /**
     * Takes a SyncDeleteMesage object from the Auth Server and queries the DB to
     * see if the File exists. If the query is successful, the File is deleted from
     * both the File DB and the associated File System and the Auth Server is
     * notified.
     * 
     * @param request DeleteMessage received from Auth Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: SYNCDELETE_REQUEST
     * @sentInstructionIDs: SYNCDELETE_SUCCESS, SYNCDELETE_FAIL, SYNCDELETE_INVALID
     */

    private void deleteFile(SyncDeleteMessage request) {
        if (request.getStatus() == SyncDeleteStatus.SYNCDELETE_REQUEST) {

            // Attempt recursive deletion from the Filesystem
            Path toBeDeleted = ReplicaFileServer.FILESTORAGEFOLDER_PATH.resolve(request.getFileCode());
            try (Stream<Path> elements = Files.walk(toBeDeleted)) {
                elements.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("ERROR! Couldn't delete" + toBeDeleted.toString());
            }

            try (PreparedStatement query = ReplicaFileServer.fileDB
                    .prepareStatement("DELETE FROM replica_file WHERE Code = ?")) {
                query.setString(1, request.getFileCode());
                query.executeUpdate();
                ReplicaFileServer.fileDB.commit();
            } catch (Exception e) {
                try {
                    ReplicaFileServer.fileDB.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            }

            // If directory was deleted
            if (!Files.exists(toBeDeleted))
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new SyncDeleteMessage(SyncDeleteStatus.SYNCDELETE_SUCCESS, null, ReplicaFileServer.SERVER_NAME,
                                ReplicaFileServer.SERVER_TOKEN, null));

            else
                MessageHelpers.sendMessageTo(this.clientSocket, new SyncDeleteMessage(SyncDeleteStatus.SYNCDELETE_FAIL,
                        null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN, null));

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket, new SyncDeleteMessage(SyncDeleteStatus.SYNCDELETE_FAIL,
                    null, ReplicaFileServer.SERVER_NAME, ReplicaFileServer.SERVER_TOKEN, null));
        }
    }
}
