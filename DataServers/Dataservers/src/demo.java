package DataServers.Dataservers.src;

import com.google.gson.Gson;
import jdk.nashorn.internal.parser.JSONParser;
import org.json.*;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;

class demo  {

    HashMap<Integer, InetAddress> systemMap;
    static HashMap<Integer, String> contents = new HashMap<>();
    static DatagramSocket sendSocket;
    static DatagramSocket recSocket;
    static DatagramSocket joinSocket;
    static DatagramSocket sendDataReqSocket;
    static DatagramSocket recDataSocket;
    static DatagramSocket sendDataRepSocket;
    static DatagramSocket recDataRepSocket;

    static int startRange;
    static int endRange;
    static int oldNeighborStartRange;
    JSONObject joindata;
    static int regPort = 9000;
    static int joinReqPort = 5006;
    static int joinReqReplyPort = 9000;
    static int sendDataCopyPort = 8500;
    static int recDataCopyPort = 10005;
    static int sendDataRepReq = 11000;
    static int recDataRepReply = 10005;
    static int sendHB = 13000;
    static int receiveHB = 14000;
    static int dataReqPort = 10001;
    static int dataRecPort = 10005;
    static String leftNeighbor, rightNeighbor, oldNeighborStart;

    public demo()   {
        systemMap = new HashMap<>();
    }

    public static void main(String[] args) throws SocketException{
        sendSocket = new DatagramSocket(joinReqReplyPort);
        joinSocket = new DatagramSocket(joinReqPort);
        recSocket = new DatagramSocket(regPort);
        recDataSocket = new DatagramSocket(recDataCopyPort);
        recDataRepSocket = new DatagramSocket(recDataRepReply);
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
         leftNeighbor = joindata.getString("left Node");
         rightNeighbor = joindata.getString("right node");
         oldNeighborStart = joindata.getString("");
        startRange = joindata.getInt("starting point");
        endRange = joindata.getInt("end point");
        oldNeighborStartRange = joindata.getInt("old starting point");

      return successfullyRegistered;
    }



    private boolean replicateData() throws UnknownHostException {
        boolean success = false;
        while (!success)    {
            JSONObject obj = new JSONObject();
            obj.append("Request_Type", "Replicate");
            obj.append("Start_Index", startRange);
            obj.append("End_Index", endRange);
            obj.append("port", "10005");
            byte[] sendBuffer = obj.toString().getBytes();
            byte[] receiveBuffer = new byte[2048];
            DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor) , regPort);
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
                    System.out.println("Replication request accepted");
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

            JSONObject sendObj = new JSONObject(contents);
            sendBuffer = sendObj.toString().getBytes();

            sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor) , regPort);
            try {
                sendDataReqSocket.send(sendPacket);
                System.out.println("Replication data sent");
            } catch (IOException e) {
                e.printStackTrace();
            }


//                for(int key: contents.keySet()) {
//                    boolean singleInstanceSuccess = false;
//                    while (!singleInstanceSuccess) {
//                        obj = new JSONObject();
//                        obj.append("Key", key);
//                        obj.append("Value", contents.get(key));
//                        sendBuffer = obj.toString().getBytes();
//                        sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(leftNeighbor), regPort);
//                        // packet being sent
//                        try {
//                            sendDataReqSocket.send(sendPacket);
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                        // receive ack
//                        receiveBuffer = new byte[2048];
//                        receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
//
//                        // receive ack for single data instance
//                        try {
//                            recDataSocket.receive(receivePacket);
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                        JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
//                        // if request accepted, break out of the loop
//                        if (recData.get("Data").equals("Successfully_Received"))
//                            singleInstanceSuccess = true;
//                    }
//                }
        }

        return success;
    }

    private boolean replicateDataReq()  {
        boolean success = false;
        byte[] receiveBuffer = new byte[2048];
        DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
        try {
            recSocket.receive(receivePacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
        if(recData.getString("Request_Type").equals("Replicate"))   {
            // replication request received

            receiveBuffer = new byte[16384];
            receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            try {
                recSocket.receive(receivePacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
            recData = new JSONObject(receivePacket.getData().toString().trim());


        }

        return success;
    }

    /**
     * This function copies the data from the left neighbor (takes in the data for its own range).
     * @return
     */
    private boolean copyData() throws UnknownHostException {
        boolean success = false;
        while(!success) {
            // send a packet for self data request
            JSONObject obj = new JSONObject();
            obj.append("Request_Type", "Copy");
            obj.append("Start_Index", startRange);
            obj.append("End_Index", endRange);
            obj.append("port", "10005");

            byte[] sendBuffer = obj.toString().getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(leftNeighbor) , regPort);
            try {
                sendDataReqSocket.send(sendPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // start to receive the data


                byte[] receiveBuffer = new byte[8192];
                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    recDataSocket.receive(receivePacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Gson gson = new Gson();
                JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
                // getting the actual contents
                contents = (HashMap<Integer, String>) gson.fromJson(recData.get("1").toString(), contents.getClass());
//                    JSONObject sendObj = new JSONObject();
//                    sendObj.append("Request_Type", recData.getInt("key"));
//                     sendBuffer = sendObj.toString().getBytes();
//                    sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , recData.getInt("port"));
//                    //sending ack for received data
//                    try {
//                        recDataSocket.send(sendPacket);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }

            HashMap<Integer, String> toBereplicated = (HashMap<Integer, String>) gson.fromJson(recData.get("1").toString(), contents.getClass());

            success = true;
        }

        return success;
    }


    private boolean recCopyRequest()    {
        boolean success = false;
        byte[] receiveBuffer = new byte[2048];
        DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);

        try {
            recSocket.receive(receivePacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
        if(recData.get("Request_Type").equals("Copy"))  {
            // copy request received
            int startRange = recData.getInt("Start_Index");
            int endRange = recData.getInt("End_Index");
            int port = recData.getInt("port");
            HashMap<Integer, String> toBeSent = new HashMap<>();

           // getting the relevant data
            for(int key: contents.keySet()) {
                if(key>= startRange && key<= endRange) {
                    toBeSent.put(key, contents.get(key));

                }

                    // data to be sent
                    JSONObject sendObj = new JSONObject(toBeSent);
                    byte[] sendBuffer = sendObj.toString().getBytes();

                    DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , port);

                    try {
                        sendSocket.send(sendPacket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


        }



        return success;
    }


}