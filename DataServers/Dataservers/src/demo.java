package DataServers.Dataservers.src;

import org.json.*;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;

class demo  {

    HashMap<Integer, InetAddress> systemMap;
    static DatagramSocket sendSocket;
    static DatagramSocket recSocket;
    static DatagramSocket joinSocket;
    JSONObject joindata;
    int regPort = 9000;
    public demo()   {
        systemMap = new HashMap<>();
    }

    public static void main(String[] args) {
        System.out.println("Hello world");
    }

    private boolean registerAsDataNode()    {
      boolean successfullyRegistered = false;

      for(int i: systemMap.keySet())    {
          if(!successfullyRegistered) {
              JSONObject obj = new JSONObject();
              obj.append("Request_Type", "Register");
              obj.append("port", "9000");
              byte[] sendBuffer = obj.toString().getBytes();
              DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(i), regPort);
              // sending join request
              try {
                  sendSocket.send(sendPacket);
              } catch (IOException e) {
                  e.printStackTrace();
              }
                // receiving join confirmation
              byte[] receiveBuffer = new byte[2048];
              DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
              try {
                  joinSocket.receive(receivePacket);
                  String data = receivePacket.getData().toString().trim();
                  joindata = new JSONObject(data);
                  successfullyRegistered = true;
              } catch (IOException e) {
                  e.printStackTrace();
              }
          }
      }
      // assumed that join request was successful
        String leftNeighbor = joindata.getString("left_neighbor");
        String rightNeighbor = joindata.getString("right_neighbor");
        


      return successfullyRegistered;
    }



}