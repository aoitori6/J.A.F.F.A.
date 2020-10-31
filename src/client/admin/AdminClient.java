package client.admin;

import java.net.InetSocketAddress;
import java.net.Socket;

import client.regular.RegularClient;
import signals.base.Message;
import signals.derived.makeadmin.*;
import signals.derived.unmakeadmin.*;
import signals.utils.MessageHelpers;

public class AdminClient extends RegularClient {

    public AdminClient(InetSocketAddress authAddr) {
        super(authAddr);
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
