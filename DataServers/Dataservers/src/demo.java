/**
 * @author: Shobhit Garg
 */

package DataServers.Dataservers.src;

import com.google.gson.Gson;
import com.sun.scenario.animation.shared.InterpolationInterval;
import jdk.nashorn.internal.parser.JSONParser;
import org.json.*;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Random;

class demo extends Thread {

    int threadID;
    HashMap<Integer, InetAddress> systemMap;
    static DatagramSocket reqSendingSocket;
    static DatagramSocket reqReceivingSocket;
    static DatagramSocket dataSendingSocket;
    static DatagramSocket dataReceivingSocket;
    static boolean heartBeatResponseReceived = false;
    static DBINterface dbiNterface = new DBINterface();
    static int startRange;
    static int endRange;
    static int oldNeighborStartRange;
    JSONObject joindata;
    static int reqSendingPort = 5006;
    static int reqReceivingPort = 9000;
    static int dataSendingPort = 11000;
    static int dataReceivingPort = 10005;
    static int heartBeatPort = 20000;
    static boolean registered = false;
    static String leftNeighbor, rightNeighbor, oldNeighborStart;

    public demo() throws UnknownHostException {
        systemMap = new HashMap<>();
        systemMap.put(1,InetAddress.getByName("129.21.159.104"));
    }

    public static void main(String[] args) throws SocketException, UnknownHostException {
        reqSendingSocket = new DatagramSocket(reqSendingPort);
        reqReceivingSocket = new DatagramSocket(reqReceivingPort);
        dataSendingSocket = new DatagramSocket(dataSendingPort);
        dataReceivingSocket = new DatagramSocket(dataReceivingPort);
        System.out.println("Hello world");
        demo d1 = new demo();
        d1.threadID = 1;// receive from server
        demo d2 = new demo();
        d2.threadID = 2;
        d2.start();

        d1.start();

    }

    private void registerAsDataNode() throws UnknownHostException {
        boolean successfullyRegistered = false;
        System.out.println("Inside register as data node");
        for(int i: systemMap.keySet())    {
            if(!successfullyRegistered) {
                JSONObject obj = new JSONObject();
                Random random = new Random();
                obj.append("flag", "Register");
                obj.append("port", 9000);
                obj.append("point", random.nextInt(360));
                byte[] sendBuffer = obj.toString().getBytes();
                System.out.println("Request sending to: " + systemMap.get(i) + " at port: " + reqSendingPort);
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(i), reqSendingPort);
                // sending join request
                try {
                    reqSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // receiving join confirmation
                byte[] receiveBuffer = new byte[2048];
                System.out.println("Now waiting to listen from the request server");
                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    reqReceivingSocket.receive(receivePacket);
                    String data = new String(receivePacket.getData(), "UTF-8");
                    joindata = new JSONObject(data);
                    successfullyRegistered = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // assumed that join request was successful

        leftNeighbor = joindata.getString("Left Node");
        rightNeighbor = joindata.getString("Right Node");

        startRange = joindata.getInt("Starting Point");
        //Integer.parseInt(joindata.getString("starting point"));
        endRange = joindata.getInt("End Point");
       // endRange = Integer.parseInt(joindata.getString("end point"));
        oldNeighborStartRange = joindata.getInt("Old Starting Point");
        System.out.println("Left neighbor: "  + leftNeighbor);
        System.out.println("Right neighbor: "  + rightNeighbor);
        System.out.println("old neighbor start range: "  + oldNeighborStartRange);
        System.out.println("Start Range: " + startRange );
        System.out.println("End Range: " + endRange);
        if(!leftNeighbor.equals("empty") && !rightNeighbor.equals("empty"))
        copyData();
        registered = true;
    }


    public void run()   {
    if(threadID == 1)   {
        try {
            System.out.println("Inside run for receive");
            receiveFromReqServers();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    else if(threadID == 2)  {
        try {
            receive();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    }

    void receiveFromReqServers() throws IOException   {
        if(!registered)  {
            registerAsDataNode();
        }
        while (true)    {
            byte[] receiveBuffer = new byte[2048];
            DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            reqReceivingSocket.receive(receivePacket);
            JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
            System.out.println("Flag: " + recData.getString("flag"));

            if(recData.getString("flag").equals("Failure Reply"))    {
                // new neighbor received
                rightNeighbor = recData.getString("New Neighbor");
                endRange = recData.getInt("End Point");


            }


        }
    }

    void receive() throws IOException {
        while (true)    {
            byte[] receiveBuffer = new byte[32768];
            DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            try {
                dataReceivingSocket.receive(receivePacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
            JSONObject recData = new JSONObject(receivePacket.getData().toString().trim());
            System.out.println("Flag: " + recData.getString("flag"));
            // copy
            if(recData.getString("flag").equals("copy"))    {
                //            // copy request received
                int startRange1 = recData.getInt("Start_Index");
                int endRange1 = recData.getInt("End_Index");
                int port = recData.getInt("port");

                // sending original data
                JSONObject sendObj = dbiNterface.getOriginalDataWithRange(new int[] {startRange1, endRange1});

                // data to be sent
                sendObj.append("flag", "copy_response");

                byte[] sendBuffer = sendObj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);

                try {
                    dataSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                dbiNterface.deleteOriginalData(new int[] {startRange1, endRange1});

                // sending data for replication

                sendObj = dbiNterface.getData("original");
                // data to be sent
                sendObj.append("flag", "replicate_data");
                sendBuffer = sendObj.toString().getBytes();
                sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);

                try {
                    dataSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            else if(recData.getString("flag").equals("copy_response"))  {
                dbiNterface.writeData("original", recData );
                // call replication function
                replicateData();

            }
            else if(recData.getString("flag").equals("replicate_data"))   {
                dbiNterface.deleteReplicationData();
                dbiNterface.writeData("replication", recData);
            }
            else if(recData.getString("flag").equals("HeartBeat"))  {
                // send heartbeat response
                JSONObject obj = new JSONObject();
                obj.put("flag", "HeartBeat OK");
                byte[] sendBuffer = obj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);
                dataSendingSocket.send(sendPacket);
            }
            else if(recData.getString("flag").equals("HeartBeat OK"))   {
                heartBeatResponseReceived = true;
            }

            else if(recData.getString("flag").equals("Get Replication Data"))   {
                // get replicated data and send
                JSONObject sendObj = dbiNterface.getData("replication");
                sendObj.put("flag", "Replicated Data");
                byte[] sendBuffer = sendObj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);
                dataSendingSocket.send(sendPacket);
            }

            else if(recData.getString("flag").equals("Replicated Data"))    {
                dbiNterface.writeData("original", recData);

                JSONObject sendObj = dbiNterface.getData("original");
                sendObj.put("flag", "New Replication Data");
                byte[] sendBuffer = sendObj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);
                dataSendingSocket.send(sendPacket);

            }
            else if(recData.getString("flag").equals("New Replication Data"))   {
                dbiNterface.deleteReplicationData();
                dbiNterface.writeData("replication", recData);
            }

        }
    }

    private void sendHeartBeat() throws UnknownHostException, SocketException, InterruptedException {
        DatagramSocket sendSock = new DatagramSocket(heartBeatPort);
        JSONObject obj = new JSONObject();
        obj.put("flag", "HeartBeat");
        byte[] sendBuffer = obj.toString().getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor) , dataReceivingPort);
        try {
            sendSock.send(sendPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // sleeping
        Thread.sleep(5000);
        if(!heartBeatResponseReceived)  {
            // right neighbor failed
             obj = new JSONObject();
             obj.put("flag", "FailureNotice");
             obj.put("FailedNode", rightNeighbor);
            rightNeighbor = null;
        sendBuffer = obj.toString().getBytes();
             sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(1) , reqSendingPort);
            try {
                reqSendingSocket.send(sendPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    private void replicateData() throws UnknownHostException {

        JSONObject obj = dbiNterface.getData("original");
        obj.append("flag", "replicate_data");
        obj.append("port", 9000);
        byte[] sendBuffer = obj.toString().getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor), dataReceivingPort);
        // sending request for replication
        try {
            dataSendingSocket.send(sendPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // receiving response for replication

    }



    /**
     * This function copies the data from the left neighbor (takes in the data for its own range).
     * @return
     */
    private void copyData() throws UnknownHostException {
        // send a packet for self data request
        JSONObject obj = new JSONObject();
        obj.append("flag", "Copy");
        obj.append("Start_Index", startRange);
        obj.append("End_Index", endRange);
        obj.append("port", "10005");

        byte[] sendBuffer = obj.toString().getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(leftNeighbor) , dataReceivingPort);
        try {
            dataSendingSocket.send(sendPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void reqDataFromRight() {
        JSONObject obj = new JSONObject();
        obj.append("flag", "");
    }



}
