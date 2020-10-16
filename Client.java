import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

final public class Client {

    private final int port;
    private final String address;

    private String name;
    private Socket authSocket;

    private Socket fileSocket;
    private String authToken;

    public Client(String address, int port) {
        this.address = address;
        this.port = port;

        try {
            authSocket = new Socket(address, port);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    /**
     * Function that sends a Message object to a Destination Socket
     * 
     * @param destination Socket to Destination Server
     * @param request     Message Object to be sent
     * @return {@code true} if Message was successfully sent to Destination,
     *         {@code false} otherwise
     */
    private boolean sendMessageTo(Socket destination, Message request) {
        ObjectOutputStream toDestination;
        try {
            toDestination = new ObjectOutputStream(destination.getOutputStream());
            toDestination.writeObject(request);
            toDestination.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            toDestination = null;
        }
    }

    /**
     * Function that receives a Message object from a Source Socket
     * 
     * @param source Socket to Source Server
     * @return {@code Message} object receives from Source Socket if connection was
     *         successful, otherwise {@code null}
     */
    private Message receiveMessageFrom(Socket source) {
        ObjectInputStream fromDestination;
        Message response;
        try {
            fromDestination = new ObjectInputStream(source.getInputStream());
            response = (Message) fromDestination.readObject();
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            fromDestination = null;
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

        if (!sendMessageTo(authSocket, new LoginMessage(LoginStatus.LOGIN_REQUEST, requestHeaders, name)))
            return false;
        requestHeaders = null;

        // Reading AuthServer's response
        Message response = receiveMessageFrom(authSocket);
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
     * @throws NullPointerException: If no Server found
     *                               <p>
     *                               Message Specs
     * @sentInstructionIDs: GET_SERVER
     * @expectedInstructionIDs: SERVER_FOUND, SERVER_NOT_FOUND
     * @expectedHeaders: addr:ServerAddress, port:ServerPort
     */
    private HashMap<String, String> fetchServerAddress(LocateServerMessage request) throws NullPointerException {

        // Sending LocateServer request
        if (!sendMessageTo(authSocket, request))
            throw new NullPointerException("ERROR. FileServer not found!");

        // Expecting LocateServer request
        Message response = receiveMessageFrom(authSocket);
        LocateServerMessage castResponse = (LocateServerMessage) response;
        response = null;

        // Parsing Response
        if (castResponse.getStatus() != LocateServerStatus.SERVER_FOUND)
            throw new NullPointerException("ERROR. FileServer not found!");

        return castResponse.getHeaders();
    }

    /**
     * @param code     File Code
     * @param savePath Path to save File to
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
        try {
            fileServerAddress = fetchServerAddress(
                    new LocateServerMessage(LocateServerStatus.GET_SERVER, null, this.name, this.authToken, true));
        } catch (Exception e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        }

        // If valid Address returned, attempt to connect to FileServer
        try {
            this.fileSocket = new Socket(fileServerAddress.get("addr"),
                    Integer.parseInt(fileServerAddress.get("port")));
        } catch (Exception e) {
            e.printStackTrace();
            return DownloadStatus.DOWNLOAD_FAIL;
        }

        // Send a DownloadRequest to the File Server
        // Expect DOWNLOAD_START if file exists and all is successful
        HashMap<String, String> requestHeaders = new HashMap<String, String>();
        requestHeaders.put("code:", code);
        if (!sendMessageTo(fileSocket,
                new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST, requestHeaders, name, this.authToken)))
            return DownloadStatus.DOWNLOAD_FAIL;

        // Parse Response
        Message response = receiveMessageFrom(fileSocket);
        DownloadMessage castResponse = (DownloadMessage) response;
        response = null;

        // If File doesn't exist
        if (castResponse.getStatus() != DownloadStatus.DOWNLOAD_START)
            return DownloadStatus.DOWNLOAD_FAIL;

        HashMap<String, String> responseHeaders = castResponse.getHeaders();
        // Check if Save Path exists
        // If not, try to create it
        savePath.resolve(responseHeaders.get("fileName"));

        if (Files.notExists(savePath))
            try {
                Files.createDirectories(savePath);
            } catch (IOException e) {
                e.printStackTrace();
            }

        // Start downloading the File

        return DownloadStatus.DOWNLOAD_SUCCESS;

    }
}