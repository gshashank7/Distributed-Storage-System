package DataServers.Dataservers.src;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.HashMap;

/**
 * Created by shobhitgarg on 4/24/17.
 */
public class TCPDemoClient {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Socket client = new Socket("129.21.22.196", 9000);

        InputStream inputStream = client.getInputStream();
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        HashMap<Integer, String> data = (HashMap<Integer, String>) objectInputStream.readObject();

        for(int key: data.keySet()) {
            System.out.println("Key: " + key + "  Value: " + data.get(key));
        }
    }
}
