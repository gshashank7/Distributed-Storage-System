package DataServers.Dataservers.src;
import com.google.gson.reflect.TypeToken;
import org.json.*;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by shobhitgarg on 4/24/17.
 */
public class JSONScratch {

    public static void main(String[] args) {

        HashMap<Integer, String> data = new HashMap<>();
        data.put(1, "Shobhit");
        JSONObject obj = new JSONObject(data);

       // System.out.println(obj);
        Gson gson = new Gson();
        String data2 = obj.getString("1");
        Type stringStringMap = new TypeToken<Map<Integer, String>>(){}.getType();
        Map<Integer,String> retMap =(Map<Integer,String> ) gson.fromJson(obj.toString(), data.getClass());

        System.out.println(retMap);

    }
}
