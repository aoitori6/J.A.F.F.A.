package fileserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Random;

import message.*;
import statuscodes.DeleteStatus;
import statuscodes.DownloadStatus;
import statuscodes.UploadStatus;

final public class FileServerHandler implements Runnable {
    private final Socket clientSocket;
    private final Connection fileDB;

    private final static String HOME = System.getProperty("user.home");

    FileServerHandler(Socket clientSocket, Connection fileDB) {
        this.clientSocket = clientSocket;
        this.fileDB = fileDB;
    }

    @Override
    /**
     * This method contains the central logic of the File Server. It listens for
     * {@code Message} objects from the Client, and handles them as required by
     * looking at the status field.
     */
    public void run() {
        // Expect a Message from the Client
        Message request = MessageHelpers.receiveMessageFrom(this.clientSocket);

        // Central Logic
        // Execute different methods after checking Message status

        switch (request.getRequestKind()) {
            case Download:
                serverDownload((DownloadMessage) request);
                break;
            case Upload:
                serverUpload((UploadMessage) request);
                break;
            case Delete:
                deleteFile((DeleteMessage) request);
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
     * sent to the Client, otherwise an error Message is returned.
     * 
     * @param request Dow received from Client
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

            // TODO: Check cases for download and time caps
            // First check if Result Set is empty
            File filePath;
            try {
                String code;
                headers.clear();
                if (queryResp.next() == false) {
                    // Implies Code doesn't exist
                    headers.put("fileName", null);
                    MessageHelpers.sendMessageTo(this.clientSocket,
                            new DownloadMessage(DownloadStatus.DOWNLOAD_FAIL, headers, "File Server", "tempServerKey"));
                    return;
                }

                else {
                    // Code exists, send response to Client
                    code = queryResp.getString("code");

                    // Getting path of File on the machine
                    // General Path expected is USER_HOME/sharenowdb/fileCode/file
                    // TODO: Make DB path mutable
                    filePath = Paths.get(HOME, "sharenowdb", code).toFile().listFiles()[0];

                    // Check if file exists, should always be the case
                    if (!filePath.exists()) {
                        System.out.println(
                                "ERROR. Critical error in File DB! File exists in MySQL DB but Path was invalid!");
                        this.clientSocket.close();
                        return;
                    }

                    headers.put("fileName", filePath.getName());
                    MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(DownloadStatus.DOWNLOAD_START,
                            headers, "File Server", "tempServerKey"));
                }
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
                return;

            } catch (Exception e) {
                e.printStackTrace();
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
                    new DownloadMessage(DownloadStatus.DOWNLOAD_FAIL, headers, "File Server", "tempServerKey"));
        }

        headers = null;
    }

    /**
     * Helper function for serverUpload. First tries to generate a random
     * alphanumeric String, that will both identify the File in the File DB and be
     * its parent folder in the File System.
     * 
     * This method is guaranteed to create a unique File code with respect to the
     * associated DB.
     * 
     * @return {@code true} if a Code was successfully generated and the requisite
     *         directory structure was created, {@code false} otherwise.
     * @throws IOException
     */
    private synchronized Path createFileLocation() throws IOException {
        // Generate a random 5-char alphanumeric String
        // 97 corresponds to 'a' and 122 to 'z'
        String tempCode;
        do {
            tempCode = new Random().ints(97, 122 + 1).limit(5)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

            // Check DB location in the File System to see if code already exists
        } while (Paths.get(HOME, "sharenowdb", tempCode).toFile().exists() == true);

        // Creating new folder named with File Code and return the same
        return Files.createDirectories(Paths.get(HOME, "sharenowdb", tempCode));
    }

    /**
     * Takes a UploadMessage object from the Client and begins receiving a File from
     * the Client. If the transfer is successful, File is added to the associated
     * File DB and a unique, sharable code is generated and sent back, otherwise an
     * error Message is returned.
     * 
     * @param request UploadMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: UPLOAD_REQUEST
     * @sentInstructionIDs: UPLOAD_START, UPLOAD_SUCCESS, UPLOAD_FAIL
     * @expectedHeaders: filename:FileName
     * @sentHeaders: filecode:FileCode
     */
    private void serverUpload(UploadMessage request) {
        // TODO: Uploading Files to a Master DB and syncing
        // TODO: Re-checking auth token
        if (request.getStatus() == UploadStatus.UPLOAD_REQUEST && request.getHeaders().get("filename") != null) {

            // First generate a Path to the upload folder
            Path fileFolder;
            String code;
            try {
                fileFolder = createFileLocation();
                code = fileFolder.getFileName().toString();
                // Create the file itself in the Folder
                fileFolder = Files.createFile(fileFolder.resolve(request.getHeaders().get("filename")));
            } catch (IOException e1) {
                e1.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey"));
                return;
            }

            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("code", code);

            // Code generated successfully, signal Client to begin transfer
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new UploadMessage(UploadStatus.UPLOAD_START, headers, "File Server", "tempServerKey"));
            headers = null;

            // Prep for File transfer
            int buffSize = 1_048_576;
            byte[] readBuffer = new byte[buffSize];
            BufferedOutputStream fileToDB = null;
            BufferedInputStream fileFromClient = null;

            try {
                // Begin connecting to file Server and establish read/write Streams
                fileToDB = new BufferedOutputStream(new FileOutputStream(fileFolder.toString()));
                fileFromClient = new BufferedInputStream(this.clientSocket.getInputStream());

                // Temporary var to keep track of read Bytes
                int _temp_c;
                while ((_temp_c = fileFromClient.read(readBuffer, 0, readBuffer.length)) != -1) {
                    fileToDB.write(readBuffer, 0, _temp_c);
                    fileToDB.flush();
                }

            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey"));
                return;
            } finally {
                readBuffer = null;
                try {
                    fileToDB.close();
                    fileFromClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // If file was successfully uploaded, add an entry to the File DB
            String statement;
            PreparedStatement addFile = null;
            try {
                statement = "INSERT INTO file(code, uploader, path) VALUES(?,?,?)";
                addFile = fileDB.prepareStatement(statement);
                addFile.setString(1, code);
                addFile.setString(2, request.getSender());
                addFile.setString(3, fileFolder.toFile().getParent());
                addFile.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey"));
                return;
            } finally {
                try {
                    addFile.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        else {
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey"));
        }

    }

    /**
     * Helper function for deleteFile. Queries the associated File DB to see if the
     * supplied File Code exists, and if the requester is allowed to delete it, it
     * marks the file for deletion in the DB. This method serializes access to the
     * File DB, and thus ensures that a given File is only deleted once.
     * 
     * @param fileDB  File Database to check against
     * @param code    File Code to be deleted
     * @param name    Name of the Client who requested deletion
     * @param isAdmin Whether the Client is an admin
     * @return
     */
    private static synchronized boolean deleteFromDB(Connection fileDB, String code, String name, boolean isAdmin) {
        PreparedStatement update;
        try {
            // Preparing Statement
            if (isAdmin) {
                update = fileDB.prepareStatement("UPDATE file SET deletable = TRUE WHERE code = ?");
                update.setString(1, code);
            } else {
                update = fileDB.prepareStatement("UPDATE file SET deletable = TRUE WHERE code = ? AND uploader = ?");
                update.setString(1, code);
                update.setString(2, name);
            }

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
     * Takes a DeleteMesage object from the Client and queries the DB to see if the
     * File exists, and if the Client can delete it. If the query is successful, the
     * File is deleted from both the File DB and the associated File System and the
     * Client is notified. If the query fails, then the Client is notified about the
     * reason for the same.
     * 
     * <p>
     * This method does not cancel any active downloads on the file to be deleted.
     * It only guarantees that the file will be marked as Deletable, and it is up to
     * other methods to respect that field until actual File deletion occurs. Files
     * present on the File System but not in the DB will be scheduled for deletion
     * by another listener.
     * 
     * @param request DeleteMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DELETE_REQUEST
     * @sentInstructionIDs: DELETE_SUCCESS, DELETE_FAIL, DELETE_INVALID
     * @sentHeaders: filecode:FileCode
     */

    private void deleteFile(DeleteMessage request) {
        if (request.getStatus() == DeleteStatus.DELETE_REQUEST) {

            // Attempt deletion from the File DB
            if (deleteFromDB(this.fileDB, request.getHeaders().get("code"), request.getSender(), request.checkAdmin()))
                MessageHelpers.sendMessageTo(this.clientSocket, new DeleteMessage(DeleteStatus.DELETE_SUCCESS, null,
                        "File Server", "tempServerKey", request.checkAdmin()));
            else {
                MessageHelpers.sendMessageTo(this.clientSocket, new DeleteMessage(DeleteStatus.DELETE_FAIL, null,
                        "File Server", "tempServerKey", request.checkAdmin()));
            }

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket, new DeleteMessage(DeleteStatus.DELETE_INVALID, null,
                    "File Server", "tempServerKey", request.checkAdmin()));
        }
    }

}
