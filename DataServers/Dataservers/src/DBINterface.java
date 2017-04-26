package DataServers.Dataservers.src;

/**
 * Created by nihkileshkshirsagar on 4/24/17.
 */

import java.net.ConnectException;
import java.sql.*;
import org.json.*;

public class DBINterface {
    Connection con = null;
    DBSettings dbs = new DBSettings();
    String url = "jdbc:mysql://"+ dbs.DBaddress + ":" + dbs.port + "/" + dbs.DBName;

    void connect() {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            con = DriverManager.getConnection(url, dbs.username, dbs.password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    JSONObject getData(String tableType){
        try {
            System.out.println("Get data for " + tableType);
            Statement stmt = con.createStatement();
            String query = "";

            if(tableType.equalsIgnoreCase("original"))
                query = "select * from " + dbs.primaryTable;
            else
                query = "select * from " + dbs.replicationTable;

            ResultSet rs = stmt.executeQuery(query);
            JSONArray resultArray = new JSONArray();

            while(rs.next()){
                JSONObject result = new JSONObject();
                result.append("article_title", rs.getString(1));
                result.append("article_content", rs.getString(2));
                result.append("author", rs.getString(5));
                result.append("image_name", rs.getString(6));
                result.append("hash", rs.getString(7));
                resultArray.put(result);
            }
            System.out.println("sending " + resultArray.length() + " records");
            if(tableType.equalsIgnoreCase("original"))
                return new JSONObject().put("originalData", resultArray);
            else
                return new JSONObject().put("replicationData", resultArray);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    JSONObject getOriginalDataWithRange(int[] range){
        try{
            System.out.println("Get original data with range [" + range[0] + " - "+ range[1] + "]");
            Statement stmt = con.createStatement();
            String query = "select * from " + dbs.primaryTable + " where hash where hash between "
                    + range[0] + " and " + range[1] + ";";
            ResultSet rs = stmt.executeQuery(query);
            JSONArray resultArray = new JSONArray();

            while(rs.next()){
                JSONObject result = new JSONObject();
                result.append("article_title", rs.getString(1));
                result.append("article_content", rs.getString(2));
                result.append("author", rs.getString(5));
                result.append("image_name", rs.getString(6));
                result.append("hash", rs.getString(7));
                resultArray.put(result);
            }
            System.out.println("sending " + resultArray.length() + " records of original data ");
        return new JSONObject().put("originalData", resultArray);
    }
    catch (SQLException e){
        e.printStackTrace();
    }
        return null;
    }

    void writeData(String tableType, JSONObject obj){
        JSONArray dataJSON = null;
        if(tableType.equalsIgnoreCase("original"))
            dataJSON = (JSONArray) obj.get("original");
        else
            dataJSON = (JSONArray) obj.get("replication");
        try {
            String insertArticle;
            if(tableType.equalsIgnoreCase("original")) {
                insertArticle = "INSERT INTO " + dbs.primaryTable +
                        " (article_title, article_content, author, image_name, hash)"
                        + " VALUES(?,?,?,?,?)";
            }
            else{
                insertArticle = "INSERT INTO " + dbs.replicationTable +
                        " (article_title, article_content, author, image_name, hash)"
                        + " VALUES(?,?,?,?,?)";
            }

            PreparedStatement psArticles = con.prepareStatement(insertArticle);

            for (int i = 0; i < dataJSON.length(); i++) {
                JSONObject row = (JSONObject) dataJSON.get(i);
                psArticles.setString(1, row.get("article_title").toString());
                psArticles.setString(2, row.get("article_content").toString());
                psArticles.setString(3, row.get("author").toString());
                psArticles.setString(4, row.get("image_name").toString());
                psArticles.setString(5, row.get("hash").toString());
                psArticles.addBatch();
            }
            psArticles.executeBatch();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    void deleteOriginalData(int[] range){
        try {
            System.out.println("Delete records for ["  + range[0] + " - "+ range[1] + "]");
            Statement stmt = con.createStatement();
            String query = "delete from " + dbs.primaryTable + " where hash between "
                    + range[0] + " and " + range[1] + ";";
            stmt.execute(query);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    void deleteReplicationData(){
        try {
            System.out.println("Delete records for replication");
            Statement stmt = con.createStatement();
            String query = "delete from " + dbs.replicationTable + ";";
            stmt.execute(query);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DBINterface dbin = new DBINterface();
        dbin.connect();
        dbin.deleteOriginalData(new int[] {0,90});
        JSONObject obj = dbin.getData("original");
        System.out.println(obj);
    }
}
