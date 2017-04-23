package DataServers.Dataservers.src;

import org.json.*;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;

class demo  {

    HashMap<Integer, InetAddress> systemMap;
    static HashMap<Integer, String> contents = new HashMap<>();
    static DatagramSocket sendSocket;
    static DatagramSocket recSocket;
    static DatagramSocket joinSocket;
    static DatagramSocket sendDataReqSocket;
    static DatagramSocket recDataSocket;
    static int startRange;
    static int endRange;
    JSONObject joindata;
    int regPort = 9000;
    int dataReqPort = 10001;
    int dataRecPort = 10005;
    static String leftNeighbor, rightNeighbor;
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
         leftNeighbor = joindata.getString("left_neighbor");
         rightNeighbor = joindata.getString("right_neighbor");
        


      return successfullyRegistered;
    }

    /**
     * This function copies the data from the left neighbor (takes in the data for its own range).
     * @return
     */
    private boolean copyData()  {
        boolean success = false;
            while(!success) {
                // send a packet for self data request
                JSONObject obj = new JSONObject();
                obj.append("Request_Type", "Copy");
                obj.append("Start_Index", startRange);
                obj.append("End_Index", endRange);
                obj.append("port", "10005");

                byte[] sendBuffer = obj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(Integer.parseInt(leftNeighbor)) , regPort);
                try {
                    sendDataReqSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // start to receive the data
                boolean done = false;
                while(!done)    {
                    byte[] receiveBuffer = new byte[8192];
                    DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                    try {
                        recDataSocket.receive(receivePacket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
                    if(recData.get("Data").equals("Done"))
                        done = true;
                    else {
                        contents.put(recData.getInt("data key"), recData.getString("Data"));
                    }

                }

                success = true;
            }

        return success;
    }

    private boolean replicateData() {
        boolean success = false;
        while (!success)    {
            JSONObject obj = new JSONObject();
            obj.append("Request_Type", "Replicate");
            obj.append("Start_Index", startRange);
            obj.append("End_Index", endRange);
            obj.append("port", "10005");
            byte[] sendBuffer = obj.toString().getBytes();
            byte[] receiveBuffer = new byte[2048];
            DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(Integer.parseInt(leftNeighbor)) , regPort);
            DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);

            boolean replicationReqAccepted = false;
            while(!replicationReqAccepted) {
                // sending request for replication
                try {
                    sendDataReqSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // receiving response for replication
                try {
                    recDataSocket.receive(receivePacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
                // if request accepted, break out of the loop
                if (recData.get("Data").equals("Accepted"))
                    replicationReqAccepted = true;

            }

            boolean done = false;
            // sending data for replication
                for(int key: contents.keySet()) {
                    boolean singleInstanceSuccess = false;
                    while (!singleInstanceSuccess) {
                        obj = new JSONObject();
                        obj.append("Key", key);
                        obj.append("Value", contents.get(key));
                        sendBuffer = obj.toString().getBytes();
                        sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(Integer.parseInt(leftNeighbor)), regPort);
                        // packet being sent
                        try {
                            sendDataReqSocket.send(sendPacket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        // receive ack
                        receiveBuffer = new byte[2048];
                        receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);

                        // receive ack for single data instance
                        try {
                            recDataSocket.receive(receivePacket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
                        // if request accepted, break out of the loop
                        if (recData.get("Data").equals("Successfully_Received"))
                            singleInstanceSuccess = true;
                    }
                }
        }

        return success;
    }


}