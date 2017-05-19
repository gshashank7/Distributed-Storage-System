/**
 * @author: Shobhit Garg
 */

package DataServers.Dataservers.src;

import org.json.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

/**
 * This class is used to create a data store. It represents a single data node in our database. It incorporates
 * failure handling, fault tolerance and replication by replicating data to the right neighbor and sending it periodic
 * heartbeats to check its status.
 */
class DataStore extends Thread {

    int threadID;
    HashMap<Integer, InetAddress> systemMap;
    static DatagramSocket reqSendingSocket;
    static DatagramSocket reqReceivingSocket;
    static DatagramSocket dataSendingSocket;
    static DatagramSocket dataReceivingSocket;
    static boolean heartBeatResponseReceived = false;
    static DBINterface dbInterface;
    static int startRange;
    static int endRange;
    static int oldNeighborStartRange;
    JSONObject joindata;
    static int reqSendingPort = 5006;
    static int reqReceivingPort = 9000;
    static int dataSendingPort = 11000;
    static int dataReceivingPort = 10005;
    static int heartBeatPort = 20000;
    static DatagramSocket sendSock;
    static boolean registered = false;
    static String leftNeighbor, rightNeighbor, oldNeighborStart;
    //InetAddress.getLocalHost().toString()  129.21.85.74
    // 129.21.37.42 domino
    //129.21.37.28 yes
    //static String IPP = "129.21.85.74";//local
    //static String IPP = "129.21.37.42";//domino
     //static String IPP = "129.21.37.28"; //yes
    static String IPP = "129.21.37.70"; //midas

    /**
     * Constructor function is used to initialize the hashmap that contains the list of all the entry point servers.
     * @throws UnknownHostException
     */
    public DataStore() throws UnknownHostException {
        systemMap = new HashMap<>();
        systemMap.put(1,InetAddress.getByName("129.21.156.120"));
    }

    /**
     *
     * main function creates three threads to receive the data from entry point server and other data servers. It uses
     * the third thread to send periodic heartbeats to the data server on its right.
     * @param args          Command line arguments
     * @throws SocketException
     * @throws UnknownHostException
     */
    public static void main(String[] args) throws SocketException, UnknownHostException {
        dbInterface = new DBINterface();
        reqSendingSocket = new DatagramSocket(reqSendingPort);
        reqReceivingSocket = new DatagramSocket(reqReceivingPort);
        dataSendingSocket = new DatagramSocket(dataSendingPort);
        dataReceivingSocket = new DatagramSocket(dataReceivingPort);
        sendSock = new DatagramSocket(heartBeatPort);
        dbInterface.connect();

        System.out.println("Hello world");
        DataStore d1 = new DataStore();
        d1.threadID = 1;// receive from server
        DataStore d2 = new DataStore();
        d2.threadID = 2;    // receive from data servers
        DataStore d3 = new DataStore(); // heartbeats
        d3.threadID = 3;
        d2.start();
        d3.start();

        d1.start();

    }

    /**
     * This function is used to register the node as one of the data nodes. After the execution of this function, the
     * data node gets its address space and information about its left and right neighbor.
     * @throws UnknownHostException
     */
    private void registerAsDataNode() throws UnknownHostException {
        boolean successfullyRegistered = false;
        System.out.println("Inside register as data node");
        for(int i: systemMap.keySet())    {
            if(!successfullyRegistered) {
                JSONObject obj = new JSONObject();
                Random random = new Random();
                obj.append("flag", "Register");
                obj.append("port", 9000);
                //InetAddress.getLocalHost().toString()  129.21.85.74
                // 129.21.37.42 domino
                //129.21.37.28 yes
                obj.append("IP", IPP);
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

    /**
     * run function handles the thread functions. It directs the threads to their respective functions
     * depending on their thread IDs.
     */
    public void run()   {
    if(threadID == 1)   {
        try {
            System.out.println("Inside run for receive");
            receiveFromReqServers();
        }  catch (IOException | SQLException e ) {
            e.printStackTrace();
        }

    }
    else if(threadID == 2)  {
        try {
            receive();
    } catch (IOException |SQLException e) {
            e.printStackTrace();
        }
    }
    else if(threadID == 3)  {
        try {
            sendHeartBeat();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    }

    /**
     * this function handles all the request received from the entry point servers.
     * @throws IOException
     * @throws SQLException
     */
    void receiveFromReqServers() throws IOException, SQLException {
        if(!registered)  {
            registerAsDataNode();
        }
        while (true)    {
            byte[] receiveBuffer = new byte[2048];
            DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            reqReceivingSocket.receive(receivePacket);
            String data = new String(receivePacket.getData(), "UTF-8");
            JSONObject recData = new JSONObject(data);
            String flag = recData.get("flag").toString();
            System.out.println("Flag received from server: " + recData.getString("flag"));

            if(flag.equals("FailureReply"))    {
                // new neighbor received
                rightNeighbor = recData.getString("NewNeighbor");
                endRange = recData.getInt("EndPoint");
                reqDataFromRight();

            }
            else if (flag.equals("Insert")) {
                // inserting an article
                dbInterface.insertArticle(recData);
                recData.put("flag", "replicateArticle");
                byte[] sendBuffer = recData.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor), dataReceivingPort);
                // sending request for replication
                try {
                    dataSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Inserted an article");

            }
            else if(flag.equals("Update"))  {
                // update an article
                dbInterface.updateArticle(recData);
                System.out.println("Updated the entry");

                recData.put("flag", "updateReplicatedArticle");
                byte[] sendBuffer = recData.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor), dataReceivingPort);
                // sending request for replication
                try {
                    dataSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            else if(flag.equals("Read"))    {
                // read an article
                JSONObject obj = dbInterface.readArticle(recData.get("Article").toString());
                    byte[] sendBuffer = obj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(1), reqSendingPort);
                // sending join request
                try {
                    reqSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Send read request data back to the server: " + systemMap.get(1));
            }


        }
    }

    /**
     * This function handles all the request received from the data servers.
     * @throws IOException
     * @throws SQLException
     */
    void receive() throws IOException, SQLException {
        while (true)    {
            byte[] receiveBuffer = new byte[32768];
            System.out.println("Inside receive ");
            DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            try {
                dataReceivingSocket.receive(receivePacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String data = new String(receivePacket.getData(), "UTF-8");
            JSONObject recData = new JSONObject(data);

            String flag = recData.get("flag").toString().substring(2, recData.get("flag").toString().length() - 2);
            System.out.println("Received flag: " + flag + " from  a data server " + receivePacket.getAddress());
            // copy
            if(flag.equalsIgnoreCase("copy"))    {
                //            // copy request received
                int startRange1 = Integer.parseInt(recData.get("Start_Index").toString().substring(2, recData.get("Start_Index").toString().length() - 2));
                int endRange1 = Integer.parseInt(recData.get("End_Index").toString().substring(2, recData.get("End_Index").toString().length() - 2));
                int port = Integer.parseInt(recData.get("port").toString().substring(2, recData.get("port").toString().length() - 2));
                System.out.println("startRange1: " + startRange1);
                System.out.println("endRange1: " + endRange1);
                // sending original data
                rightNeighbor = receivePacket.getAddress().toString();
                rightNeighbor = rightNeighbor.substring(1, rightNeighbor.length() );
                System.out.println(rightNeighbor);
                JSONObject sendObj = dbInterface.getOriginalDataWithRange(new int[] {startRange1, endRange1});

                // data to be sent
                sendObj.append("flag", "copy_response");

                byte[] sendBuffer = sendObj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);

                try {
                    dataSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Sending copy data");
                dbInterface.deleteOriginalData(new int[] {startRange1, endRange1});

                // sending data for replication

                sendObj = dbInterface.getData("original");
                JSONObject obj2 = new JSONObject();
                obj2.put("replicationData", sendObj.getJSONArray("originalData"));
                obj2.append("flag", "replicate_data");
                obj2.append("port", 9000);
                System.out.println("sending data for replication to the new right node");
                // data to be sent
                sendBuffer = obj2.toString().getBytes();
                sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);

                try {
                    dataSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            else if(flag.equals("copy_response"))  {
                dbInterface.writeData("original", recData );
                // call replication function
                replicateData();

            }
            else if(flag.equals("replicate_data"))   {
                dbInterface.deleteReplicationData();
                System.out.println("Received data for Replication: " + recData);
                dbInterface.writeData("replication", recData);
            }
            else if(flag.equals("artBe"))  {
                // send heartbeat response

                JSONObject obj = new JSONObject();
                obj.put("flag", "HeartBeat OK");
                byte[] sendBuffer = obj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);
                dataSendingSocket.send(sendPacket);
                System.out.println("Heartbeat response sent to : " + receivePacket.getAddress().toString());
            }
            else if(flag.equals("artBeat "))   {
                System.out.println("Heartbeat response received");
                heartBeatResponseReceived = true;
            }

            else if(flag.equals("t Replication Da"))   {
                // get replicated data and send
                System.out.println("Sending the replication data to the new left neighbor");
                leftNeighbor = receivePacket.getAddress().toString();
                leftNeighbor = leftNeighbor.substring(1, leftNeighbor.length());
                JSONObject sendObj = dbInterface.getData("replication");
                JSONObject obj2 = new JSONObject();
                obj2.put("originalData", sendObj.getJSONArray("replicationData"));
                obj2.put("flag", "Replicated Data");
                obj2.append("port", 9000);
                //sendObj.put("flag", "Replicated Data");
                byte[] sendBuffer = obj2.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);
                dataSendingSocket.send(sendPacket);
            }

            else if(flag.equals("plicated Da"))    {
                // replication data
                System.out.println("Replication data received from the new right neighbor: " + recData);
                dbInterface.writeData("original", recData);

                JSONObject sendObj = dbInterface.getData("original");
                sendObj.put("flag", "New Replication Data");
                byte[] sendBuffer = sendObj.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, receivePacket.getAddress() , dataReceivingPort);
                dataSendingSocket.send(sendPacket);

            }
            else if(flag.equals("New Replication Data"))   {
                dbInterface.deleteReplicationData();
                dbInterface.writeData("replication", recData);
            }
            else if(flag.equalsIgnoreCase("plicateArtic"))  {
                // replicating article
                dbInterface.insertArticleInReplication(recData);
            }
            else if(flag.equalsIgnoreCase("dateReplicatedArtic"))   {
                dbInterface.updateArticleInReplication(recData);
            }

        }
    }

    /**
     * this function is used to incorporate heartbeats functionality. In that, it sends periodic heartbeats to its
     * right neighbor and waits for a response. If it does not receives a response within certain time, it informs the
     * entry point server about the node failure and then communicates with the new right neighbor to get the failed node's
     * data.
     * @throws UnknownHostException
     * @throws SocketException
     * @throws InterruptedException
     */
    private void sendHeartBeat() throws UnknownHostException, SocketException, InterruptedException {
        while (true) {
            Thread.sleep(2000);
            if(rightNeighbor!= null && !rightNeighbor.equalsIgnoreCase("empty"))
            {
                heartBeatResponseReceived = false;
            System.out.println("sending heartbeat");

            JSONObject obj = new JSONObject();
            obj.put("flag", "HeartBeat");
            byte[] sendBuffer = obj.toString().getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor), dataReceivingPort);
            try {
                sendSock.send(sendPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
            // sleeping
            Thread.sleep(5000);
            if (!heartBeatResponseReceived) {
                // right neighbor failed
                obj = new JSONObject();
                obj.put("flag", "FailureNotice");
                System.out.println("Right failed node: " + rightNeighbor);
                obj.append("FailedNode", rightNeighbor);
                obj.append("IP", IPP);
                obj.append("port", 9000);
                rightNeighbor = null;
                sendBuffer = obj.toString().getBytes();
                sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(1), reqSendingPort);
                try {
                    reqSendingSocket.send(sendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        else
                System.out.println(rightNeighbor);
    }
    }

    /**
     * this function is used to replicate the node's data on the right node.
     * @throws UnknownHostException
     */
    private void replicateData() throws UnknownHostException {

        JSONObject obj = dbInterface.getData("original");
        JSONObject obj2 = new JSONObject();
        obj2.put("replicationData", obj.getJSONArray("originalData"));
        obj2.append("flag", "replicate_data");
        obj2.append("port", 9000);
        byte[] sendBuffer = obj2.toString().getBytes();
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
        obj.append("Start_Index", String.valueOf(startRange));
        obj.append("End_Index", String.valueOf(endRange));
        obj.append("port", "10005");
        System.out.println("Inside copy data");

        byte[] sendBuffer = new byte[0];
        try {
            System.out.println(obj.toString());
            sendBuffer = obj.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(leftNeighbor) , dataReceivingPort);
        try {
            dataSendingSocket.send(sendPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * this function is used to request the data from the right node.
     * @throws UnknownHostException
     */
    void reqDataFromRight() throws UnknownHostException {
        JSONObject obj = new JSONObject();
        obj.put("flag", "Get Replication Data");
        byte[] sendBuffer = new byte[0];
        try {
            System.out.println(obj.toString());
            sendBuffer = obj.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(rightNeighbor) , dataReceivingPort);
        try {
            dataSendingSocket.send(sendPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        replicateData();


    }



}
