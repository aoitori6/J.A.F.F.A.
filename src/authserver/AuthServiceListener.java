package authserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;

final class AuthServiceListener implements Runnable {

    private ExecutorService executionPool;
    private ServerSocket authServiceSocket;
    private Connection authDB;

    AuthServiceListener(ServerSocket authServiceSocket, ExecutorService executionPool, Connection authDB) {
        this.authServiceSocket = authServiceSocket;
        this.executionPool = executionPool;
        this.authDB = authDB;
    }

    @Override
    public void run() {
        // Listening for new Server connections
        while (!executionPool.isShutdown()) {
            try {
                this.executionPool.execute(new AuthServiceVerifyHandle(this.authServiceSocket.accept(), this.authDB));
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
