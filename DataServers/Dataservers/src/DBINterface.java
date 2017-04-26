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

    JSONObject getOriginalTableData(){
        try {
            Statement stmt = con.createStatement();
            String query = "select * from " + dbs.primaryTable;
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
            return new JSONObject().put("originalData", resultArray);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    JSONObject getOriginalTableData(int[] range){
        return new JSONObject();
    }

    void writeOriginalData(JSONObject obj){

    }

    public static void main(String[] args) {
        DBINterface dbin = new DBINterface();
        dbin.connect();
        dbin.getOriginalTableData();
    }
}
