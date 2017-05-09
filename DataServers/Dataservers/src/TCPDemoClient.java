package DataServers.Dataservers.src;

import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.util.HashMap;

/**
 * Created by shobhitgarg on 4/24/17.
 */
public class TCPDemoClient {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        DatagramSocket socket = new DatagramSocket(8006);
        byte[] buffer = new byte[2048];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.receive(packet);
        String data = new String(packet.getData(), "UTF-8");
        JSONObject obj = new JSONObject(data);
        System.out.println(data);

        }
    }

