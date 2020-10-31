package adminclient;

import java.util.ArrayList;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import client.Client;
import message.*;
import misc.FileInfo;
import statuscodes.*;

public class Admin extends Client {

    public Admin(InetSocketAddress authAddr) {
        super(authAddr);
    }

    /**
     * @return A list of available File Information from the File DB
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
            if (!MessageHelpers.sendMessageTo(authSocket,
                    new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_REQUEST, null, this.name, this.authToken)))
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
