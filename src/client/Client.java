package client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import message.*;
import statuscodes.*;

final public class Client {

    private String name;
    private Socket authSocket;

    private Socket fileSocket;
    private String authToken;

    public Client(String address, int port) {
        try {
            authSocket = new Socket(address, port);
        } catch (Exception e) {
            e.printStackTrace();
            return;
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

    public boolean logIn(String name, String pass) {
        // Sending login Message with Username and Password as Headers
        HashMap<String, String> requestHeaders = new HashMap<String, String>();
        requestHeaders.put("pass", pass);

        if (!MessageHelpers.sendMessageTo(authSocket,
                new LoginMessage(LoginStatus.LOGIN_REQUEST, requestHeaders, name)))
            return false;
        requestHeaders = null;

        // Reading AuthServer's response
        Message response = MessageHelpers.receiveMessageFrom(authSocket);
        LoginMessage castResponse = (LoginMessage) response;
        response = null;

        // Parsing AuthServer's response
        if (castResponse.getStatus() != LoginStatus.LOGIN_SUCCESS)
            return false;

        // Setting authToken if true
        HashMap<String, String> responseHeaders = castResponse.getHeaders();
        this.authToken = responseHeaders.get("authToken");
        this.name = name;

        return true;
    }

    /**
     * 
     * @param request Kind of Server required
     * @return HashMap containing addr:ServerAddress, port:ServerPort pairs
     * @throws NullPointerException: If no File Server found
     *                               <p>
     *                               Message Specs
     * @sentInstructionIDs: GET_SERVER
     * @expectedInstructionIDs: SERVER_FOUND, SERVER_NOT_FOUND
     * @expectedHeaders: addr:ServerAddress, port:ServerPort (both null if no File
     *                   Server found)
     */
    private HashMap<String, String> fetchServerAddress(LocateServerMessage request) throws NullPointerException {

        // Sending LocateServer request
        if (!MessageHelpers.sendMessageTo(authSocket, request))
            throw new NullPointerException("ERROR. FileServer not found!");

        // Expecting LocateServer request
        Message response = MessageHelpers.receiveMessageFrom(authSocket);
        LocateServerMessage castResponse = (LocateServerMessage) response;
        response = null;

        // Parsing Response
        if (castResponse.getStatus() != LocateServerStatus.SERVER_FOUND)
            throw new NullPointerException("ERROR. FileServer not found!");

        return castResponse.getHeaders();
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
     * @sentHeaders: code:code
     * @expectedHeaders: fileName:FileName
     */
    public DownloadStatus downloadFile(String code, Path savePath) {
        // Send a LocateServerStatus to the Central Server
        // with isDownloadRequest set to true

        // Expect addr:ServerAddress, port:ServerPort if Successful
        HashMap<String, String> fileServerAddress;
        System.out.println("Trying to Fetch Server Address");
        try {
            fileServerAddress = fetchServerAddress(
                    new LocateServerMessage(LocateServerStatus.GET_SERVER, null, this.name, this.authToken, true));
        } catch (Exception e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        }
        System.out.println("Fetched Server Address");

        // If valid Address returned, attempt to connect to FileServer
        System.out.println("Trying to connect to Address");
        try {
            this.fileSocket = new Socket(fileServerAddress.get("addr"),
                    Integer.parseInt(fileServerAddress.get("port")));
        } catch (Exception e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        }
        System.out.println("Connected to Address");

        // Send a DownloadRequest to the File Server
        // Expect DOWNLOAD_START if file exists and all is successful
        System.out.println("Trying to send a request to Address");
        HashMap<String, String> requestHeaders = new HashMap<String, String>();
        requestHeaders.put("code:", code);
        if (!MessageHelpers.sendMessageTo(fileSocket,
                new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST, requestHeaders, name, this.authToken)))
            return DownloadStatus.DOWNLOAD_FAIL;
        System.out.println("Sent a request to Address");

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
        System.out.println(responseHeaders.get("fileName"));
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
        try {
            // Begin connecting to file Server and establish read/write Streams
            fileFromServer = new BufferedInputStream(fileSocket.getInputStream());
            fileOnClient = new BufferedOutputStream(new FileOutputStream(savePath.toString()));

            // Temporary var to keep track of read Bytes
            int _temp_c;
            while ((_temp_c = fileFromServer.read(writeBuffer, 0, writeBuffer.length)) != -1) {
                fileOnClient.write(writeBuffer, 0, _temp_c);
                fileOnClient.flush();
            }

            // File successfully downloaded
            fileOnClient.close();
            return DownloadStatus.DOWNLOAD_SUCCESS;

        } catch (IOException e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        } finally {
            writeBuffer = null;
            fileFromServer = null;
            fileOnClient = null;
        }

    }
}