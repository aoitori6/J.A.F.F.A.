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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import misc.FileInfo;
import message.*;
import statuscodes.DeleteStatus;
import statuscodes.DownloadStatus;
import statuscodes.FileDetailsStatus;
import statuscodes.UploadStatus;

final public class ReplicaFileServerHandler implements Runnable {
    private final Socket clientSocket;
    private final Connection fileDB;

    private final static String HOME = System.getProperty("user.home");

    ReplicaFileServerHandler(Socket clientSocket, Connection fileDB) {
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
                    return;
                }

                String downloadsRemaning = queryResp.getString("Downloads_Remaining");
                String timestamp = queryResp.getString("Deletion_Timestamp");
                int currentThreads = queryResp.getInt("Current_Threads");
                boolean flag = false;

                // Check if timestamp has been exceeded
                if (timestamp != null) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");
                    LocalDateTime deletionTimestamp = LocalDateTime.parse(timestamp, formatter);
                    if (LocalDateTime.now(ZoneId.of("UTC")).isAfter(deletionTimestamp))
                        flag = true;
                }

                // Check if downloadsRemaning is 0
                if (downloadsRemaning != null) {
                    if (Integer.parseInt(downloadsRemaning) <= 0 && currentThreads == 0)
                        flag = true;
                    // If downloadsRemaning is 0 but currentThreads are not 0
                    else if (Integer.parseInt(downloadsRemaning) <= 0) {
                        headers.put("fileName", null);
                        MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(
                                DownloadStatus.DOWNLOAD_FAIL, headers, "File Server", "tempServerKey"));
                        return;
                    }
                }

                // If any of the above is true, mark the file for deletion
                PreparedStatement update = null;
                if (flag) {
                    try {
                        update = fileDB.prepareStatement("UPDATE file SET deletable = TRUE WHERE code = ?");
                        update.setString(1, code);
                        update.executeUpdate();
                        headers.put("fileName", null);
                        MessageHelpers.sendMessageTo(this.clientSocket, new DownloadMessage(
                                DownloadStatus.DOWNLOAD_FAIL, headers, "File Server", "tempServerKey"));
                    } catch (Exception e) {
                        e.printStackTrace();

                    } finally {
                        update = null;
                    }
                    return;
                }

                // If all is successful, reduce downloadsRemaning and increase currentThreads
                try {
                    update = fileDB.prepareStatement(
                            "UPDATE file SET Downloads_Remaining = Downloads_Remaining - 1, Current_Threads = Current_Threads + 1 WHERE Code = ?");
                    update.setString(1, code);
                    update.executeUpdate();
                } catch (Exception e) {
                    e.printStackTrace();

                } finally {
                    update = null;
                }

                headers.put("fileName", filePath.getName());
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_START, headers, "File Server", "tempServerKey"));

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

                // Reduce the currentThreads
                PreparedStatement update = null;
                try {
                    update = fileDB
                            .prepareStatement("UPDATE file SET Current_Threads = Current_Threads - 1 WHERE Code = ?");
                    update.setString(1, request.getHeaders().get("code"));
                    update.executeUpdate();
                } catch (Exception e) {
                    e.printStackTrace();

                } finally {
                    update = null;
                }
                return;

            } catch (Exception e) {
                e.printStackTrace();

                // File failed to download, increase downloadsRemaining and reduce
                // currentThreads
                PreparedStatement update = null;
                try {
                    update = fileDB.prepareStatement(
                            "UPDATE file SET Downloads_Remaining = Downloads_Remaining + 1, Current_Threads = Current_Threads - 1 WHERE Code = ?");
                    update.setString(1, request.getHeaders().get("code"));
                    update.executeUpdate();
                } catch (Exception e1) {
                    e1.printStackTrace();

                } finally {
                    update = null;
                }
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
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey", null));
                return;
            }

            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("code", code);

            // Code generated successfully, signal Client to begin transfer
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new UploadMessage(UploadStatus.UPLOAD_START, headers, "File Server", "tempServerKey", null));
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
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey", null));
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
            Integer downloadCap = null;
            String timestamp = request.getHeaders().get("timestamp");

            try {
                downloadCap = Integer.parseInt(request.getHeaders().get("downloadCap"));
            } catch (NumberFormatException e) {
                downloadCap = null;
            }

            String statement;
            PreparedStatement addFile = null;
            try {
                statement = "INSERT INTO file(code, uploader, filename, Downloads_Remaining, Deletion_Timestamp) VALUES(?,?,?,?,?)";
                addFile = fileDB.prepareStatement(statement);
                addFile.setString(1, code);
                addFile.setString(2, request.getSender());
                addFile.setString(3, fileFolder.toFile().getName());
                if (downloadCap != null)
                    addFile.setInt(4, downloadCap);
                else
                    addFile.setNull(4, java.sql.Types.SMALLINT);
                addFile.setString(5, timestamp);
                addFile.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.clientSocket,
                        new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey", null));
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
                    new UploadMessage(UploadStatus.UPLOAD_FAIL, null, "File Server", "tempServerKey", null));
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
                        null, "File Server", "tempServerKey", request.checkAdmin()));
            else {
                MessageHelpers.sendMessageTo(this.clientSocket, new DeleteMessage(DeleteStatus.DELETE_FAIL, null,
                        null, "File Server", "tempServerKey", request.checkAdmin()));
            }

        } else {
            MessageHelpers.sendMessageTo(this.clientSocket, new DeleteMessage(DeleteStatus.DELETE_INVALID, null,
                    null, "File Server", "tempServerKey", request.checkAdmin()));
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
