package authserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import message.AuthMessage;
import message.Message;
import statuscodes.AuthStatus;

public class AuthServiceVerifyHandle implements Runnable {
    private Socket requester;
    private Connection authDB;

    public AuthServiceVerifyHandle(Socket requester, Connection authDB) {
        this.requester = requester;
        this.authDB = authDB;
    }

    @Override
    public void run() {
        try (ObjectOutputStream toSocket = new ObjectOutputStream(this.requester.getOutputStream());
                ObjectInputStream fromSocket = new ObjectInputStream(this.requester.getInputStream());) {
            toSocket.flush();

            Message request = (Message) fromSocket.readObject();
            AuthMessage castResp = (AuthMessage) request;
            request = null;

            if (castResp.getStatus() != AuthStatus.AUTH_CHECK) {
                toSocket.writeObject(
                        new AuthMessage(AuthStatus.AUTH_CHECKFAIL, null, "Auth Server", null, null, false));
                toSocket.flush();
                return;
            }

            try (PreparedStatement query = authDB.prepareStatement(
                    "SELECT Auth_Code FROM client WHERE Username = ? AND Auth_Code = ? AND Login_Status = 'ONLINE' AND Admin_Status = ?");) {

                query.setString(1, castResp.getClientName());
                query.setString(2, castResp.getAuthToken());
                query.setBoolean(3, castResp.getIfAdmin());

                ResultSet queryResp = query.executeQuery();

                if (queryResp.next()) {

                    toSocket.writeObject(new AuthMessage(AuthStatus.AUTH_VALID, null, "Auth Server",
                            castResp.getClientName(), castResp.getAuthToken(), queryResp.getBoolean("Admin_Status")));
                    toSocket.flush();
                    return;

                }

                else {
                    toSocket.writeObject(
                            new AuthMessage(AuthStatus.AUTH_INVALID, null, "Auth Server", null, null, false));
                    toSocket.flush();
                }
                this.authDB.commit();

            }

            catch (Exception e) {

                e.printStackTrace();
                toSocket.writeObject(
                        new AuthMessage(AuthStatus.AUTH_CHECKFAIL, null, "Auth Server", null, null, false));
                toSocket.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                requester.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
