package DataServers.Dataservers.src;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Nihkilesh Kshirsagar on 4/24/17.
 */

public class DBSettings {
    String DBName = "wikiRIT";
    //domino.cs.rit.edu
    //yes.cs.rit.edu
    //localhost

    //String DBaddress = "localhost";//local
    //String DBaddress = "domino.cs.rit.edu";//domino
    //String DBaddress = "yes.cs.rit.edu";//yes
    String DBaddress = "midas.cs.rit.edu";//midas
    String port = "3306";
    String primaryTable = "articles";
    String replicationTable = "articles_replication";
    String username = "root";
    String password = "root123";

    public DBSettings() throws UnknownHostException {
    }
}
