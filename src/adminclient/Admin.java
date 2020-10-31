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

    /**
     * Function that takes a Client username, and if valid grants them Admin status.
     * If the Client is already an Admin, there is no effect.
     * 
     * @param clientToAdmin Name of Client to be granted Admin permissions
     * @return {@code true} if Client was granted Admin permissions (or was already
     *         an Admin), {@code false} otherwise
     * 
     *         <p>
     *         Message Specs
     * @sentInstructionIDs: MAKEADMIN_REQUEST
     * @expectedInstructionIDs: MAKEADMIN_SUCCESS, MAKEADMIN_FAIL
     */
    public boolean makeUserAdmin(String clientToAdmin) {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Send a MakeAdminRequest to the Auth Server
            // Expect MAKEADMIN_START if User exists and was made an Admin (or was already
            // one)
            MessageHelpers.sendMessageTo(authSocket, new MakeAdminMessage(MakeAdminStatus.MAKEADMIN_REQUEST,
                    clientToAdmin, null, this.name, this.authToken, this.isAdmin));

            Message resp = (Message) MessageHelpers.receiveMessageFrom(authSocket);
            MakeAdminMessage castResp = (MakeAdminMessage) resp;
            resp = null;

            if (castResp.getStatus() == MakeAdminStatus.MAKEADMIN_SUCCESS)
                return true;
            else
                return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Function that takes a Client username, and if valid revokes Admin status. If
     * the Client is not an Admin, there is no effect.
     * 
     * @param clientToAdmin Name of Client to be stripped of Admin permissions
     * @return {@code true} if Client was stripped of Admin permissions (or wasn't
     *         an Admin), {@code false} otherwise
     * 
     *         <p>
     *         Message Specs
     * @sentInstructionIDs: UNMAKEADMIN_REQUEST
     * @expectedInstructionIDs: UNMAKEADMIN_SUCCESS, UNMAKEADMIN_FAIL
     */
    public boolean unMakeUserAdmin(String adminToClient) {
        try (Socket authSocket = new Socket(this.authAddr.getAddress(), this.authAddr.getPort());) {
            // Send a MakeAdminRequest to the Auth Server
            // Expect MAKEADMIN_START if User exists and was made an Admin (or was already
            // one)
            MessageHelpers.sendMessageTo(authSocket, new UnMakeAdminMessage(UnMakeAdminStatus.UNMAKEADMIN_REQUEST,
                    adminToClient, null, this.name, this.authToken, this.isAdmin));

            Message resp = (Message) MessageHelpers.receiveMessageFrom(authSocket);
            UnMakeAdminMessage castResp = (UnMakeAdminMessage) resp;
            resp = null;

            if (castResp.getStatus() == UnMakeAdminStatus.UNMAKEADMIN_SUCCESS)
                return true;
            else
                return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
