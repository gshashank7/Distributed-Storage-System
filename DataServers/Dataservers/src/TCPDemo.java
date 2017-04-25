package DataServers.Dataservers.src;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/**
 * Created by shobhitgarg on 4/24/17.
 */
public class TCPDemo {
static ServerSocket serverSocket;
    static Socket socket;
int serverPort = 9000;
int clientPort = 10000;
    public static void main(String[] args) throws IOException {
        HashMap<Integer, String > data = new HashMap<>();
        data.put(1, "Shobhit");
        data.put(2, "Nikhilesh");
        serverSocket = new ServerSocket(9000);
        Socket server = serverSocket.accept();
        OutputStream outputStream = new ObjectOutputStream(server.getOutputStream());
        ObjectOutputStream mapStream = new ObjectOutputStream(outputStream);
        mapStream.writeObject(data);

        server.close();

    }
}
