package authserver;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import message.AuthMessage;
import message.MessageHelpers;
import statuscodes.AuthStatus;

public class AuthServerVerifyHandle implements Runnable {
    private Socket fileServer;
    private Connection authDB;

    public AuthServerVerifyHandle(Socket fileServer, Connection authDB) {
        this.fileServer = fileServer;
        this.authDB = authDB;
    }

    @Override
    public void run() {
        try (ObjectInputStream fromFileServer = new ObjectInputStream(fileServer.getInputStream());
                ObjectOutputStream toFileServer = new ObjectOutputStream(fileServer.getOutputStream());) {

            AuthMessage received;
            PreparedStatement query;
            ResultSet queryResp;

            while (fileServer.isClosed() || authDB.isClosed()) {
                received = (AuthMessage) fromFileServer.readObject();

                if (received.getStatus() != AuthStatus.AUTH_CHECK)
                    throw new IllegalArgumentException();

                query = authDB.prepareStatement(
                        "SELECT Auth_Code FROM client WHERE Username = ? AND Login_Status = 'ONLINE'");
                query.setString(1, received.getClientName());
                queryResp = query.executeQuery();

                if (queryResp.next())
                    if (queryResp.getString("Auth_Code").equals(received.getAuthToken())) {
                        toFileServer.writeObject(new AuthMessage(AuthStatus.AUTH_VALID, null, "Auth Server",
                                received.getClientName(), received.getAuthToken()));
                        continue;
                    }
                toFileServer.writeObject(new AuthMessage(AuthStatus.AUTH_INVALID, null, "Auth Server",
                        received.getClientName(), received.getAuthToken()));

                query.close();
            }

        } catch (Exception e) {

            e.printStackTrace();
            MessageHelpers.sendMessageTo(this.fileServer,
                    new AuthMessage(AuthStatus.AUTH_CHECKFAIL, null, "Auth Server", null, null));
            return;

        } finally {

            try {
                authDB.close();
                fileServer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

}
