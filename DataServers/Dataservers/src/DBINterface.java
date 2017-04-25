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

    void connect() {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String url = "jdbc:mysql:// "+ dbs.DBaddress + ":" + dbs.port + "/" + dbs.DBName;
            con = DriverManager.getConnection(url, dbs.username, dbs.password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    JSONObject getData(String articleTitle){
        try {
            Statement stmt = con.createStatement();
            String query = "select * from " + dbs.primaryTable + " where article=" + articleTitle;
            ResultSet rs = stmt.executeQuery(query);
            JSONObject result = new JSONObject();

            while(rs.next()){
                result.append("article_title", rs.getString(1));
                result.append("article_content", rs.getString(2));
                result.append("author", rs.getString(5));
                result.append("image_name", rs.getString(6));
            }
            System.out.println(result.get("article_title"));
            return result;
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        DBINterface dbin = new DBINterface();
        dbin.connect();
        dbin.getData("B. Thomas Golisano College of Computing and Information Sciences");
    }
}
