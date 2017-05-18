package DataServers.Dataservers.src;

/**
 * Created by nihkileshkshirsagar on 4/24/17.
 */

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.sql.*;
import org.json.*;

class DBINterface {
    Connection con = null;
     Connection conCreate = null;
    DBSettings dbs = new DBSettings();
    String CreateDBurl = "jdbc:mysql://"+ dbs.DBaddress + ":" + dbs.port;
    String url = "jdbc:mysql://"+ dbs.DBaddress + ":" + dbs.port + "/" + dbs.DBName;

    DBINterface() throws UnknownHostException {
    }

    void connect() {
        try {
            System.out.println("In connect: local host: " + dbs.DBaddress);
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conCreate = DriverManager.getConnection(CreateDBurl, dbs.username, dbs.password);
            Statement stmtCreate = conCreate.createStatement();
            stmtCreate.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbs.DBName);
            conCreate.close();
            con = DriverManager.getConnection(url, dbs.username, dbs.password);
            System.out.println("DB created");
            Statement stmt = con.createStatement();
            stmt.executeUpdate("create table IF NOT EXISTS " + dbs.primaryTable +"  (article_title VARCHAR (500), article_content TEXT, author VARCHAR (200),image_name VARCHAR (200), hash INT )");
            stmt.executeUpdate("create table IF NOT EXISTS " + dbs.replicationTable+ " (article_title VARCHAR (500), article_content TEXT, author VARCHAR (200),image_name VARCHAR (200), hash INT)");
        } catch (Exception e) {
            e.printStackTrace();
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
                result.append("author", rs.getString(3));
                result.append("image_name", rs.getString(4));
                result.append("hash", rs.getInt(5));
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
            String query = "select * from " + dbs.primaryTable + " where hash between "
                    + range[0] + " and " + range[1] + ";";
            ResultSet rs = stmt.executeQuery(query);
            JSONArray resultArray = new JSONArray();

            while(rs.next()){
                JSONObject result = new JSONObject();
                result.append("article_title", rs.getString(1));
                result.append("article_content", rs.getString(2));
                result.append("author", rs.getString(3));
                result.append("image_name", rs.getString(4));
                result.append("hash", rs.getInt(5));
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
            dataJSON = (JSONArray) obj.get("originalData");
        else
            dataJSON = (JSONArray) obj.get("replicationData");
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
                System.out.println(row.get("article_title").toString());
                System.out.println(row.get("article_content").toString());
                System.out.println(row.get("author").toString());
                System.out.println(row.get("image_name").toString());
                System.out.println(row.get("hash").toString());
                psArticles.setString(1, row.get("article_title").toString().substring(2, row.get("article_title").toString().length() - 2));
                psArticles.setString(2, row.get("article_content").toString().substring(2, row.get("article_content").toString().length() - 2));
                psArticles.setString(3, row.get("author").toString().substring(1, row.get("author").toString().length() - 1));
                psArticles.setString(4, row.get("image_name").toString().substring(1, row.get("image_name").toString().length() - 1));
                psArticles.setInt(5, Integer.parseInt(row.get("hash").toString().substring(1, row.get("hash").toString().length() - 1)));
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


    JSONObject readArticle(String title) throws SQLException {
        Statement stmt = con.createStatement();
        String query = "Select * from " + dbs.primaryTable + " where article_title = '" + title + "'";
        ResultSet rs =  stmt.executeQuery(query);
        while (rs.next()) {
            JSONObject obj = new JSONObject();
            obj.put("flag", "ArticleResponse");
            obj.put("Article", rs.getString(1));
            obj.put("Content", rs.getString(2));

            return obj;
        }
        return new JSONObject();
    }


    void updateArticle(JSONObject obj) throws SQLException {
        Statement stmt = con.createStatement();
        String query = "Update  " + dbs.primaryTable + " set article_content = '" + obj.get("Content").toString() + "' where article_title = '" + obj.getString("Article").toString() + "'";
        stmt.executeUpdate(query);

    }

    void updateArticleInReplication(JSONObject obj) throws SQLException {
        Statement stmt = con.createStatement();
        String query = "Update  " + dbs.replicationTable + " set article_content = '" + obj.get("Content").toString() + "' where article_title = '" + obj.getString("Article").toString() + "'";
        stmt.executeUpdate(query);

    }

    void insertArticle(JSONObject obj) throws SQLException {
        Statement stmt = con.createStatement();
        String query = "Insert into " + dbs.primaryTable+ "(article_title, article_content, hash) values  ( '" + obj.getString("Article").toString()
                + "', '" + obj.get("Content").toString() + "'" + ", " + Integer.parseInt(String.valueOf(obj.get("Hash"))) + ")" ;
        stmt.executeUpdate(query);
    }

    void insertArticleInReplication(JSONObject obj) throws SQLException {
        Statement stmt = con.createStatement();
        String query = "Insert into " + dbs.replicationTable+ "(article_title, article_content, hash) values  ( '" + obj.getString("Article").toString()
                + "', '" + obj.get("Content").toString() + "'" + ", " + Integer.parseInt(String.valueOf(obj.get("Hash"))) + ")" ;
        stmt.executeUpdate(query);
    }


//
//    public static void main(String[] args) {
//        DBINterface dbin = new DBINterface();
//        dbin.connect();
//        dbin.deleteOriginalData(new int[] {0,90});
//        JSONObject obj = dbin.getData("original");
//        System.out.println(obj);
//    }

}
