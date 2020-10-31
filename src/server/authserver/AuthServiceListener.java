package server.authserver;

import java.io.IOException;
import java.net.ServerSocket;

final class AuthServiceListener implements Runnable {

    private ServerSocket authServiceSocket;

    AuthServiceListener(ServerSocket authServiceSocket) {
        this.authServiceSocket = authServiceSocket;
    }

    @Override
    public void run() {
        // Listening for new Server connections
        while (!AuthServer.executionPool.isShutdown()) {
            try {
                AuthServer.executionPool.execute(new AuthServiceVerifyHandle(this.authServiceSocket.accept()));
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
        }

        try {
            this.authServiceSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
