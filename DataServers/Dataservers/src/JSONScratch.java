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
        HashMap<Integer, String> data1 = new HashMap<>();
        data.put(1, "Shobhit");
        data.put(11, "asas");
        data1.put(2, "Garg");
        data1.put(22, "asdasdsadfa");
//        JSONObject obj1 = new JSONObject(data);
//        JSONObject obj2 = new JSONObject(data1);
        JSONObject obj = new JSONObject();
        obj.append("1", data);
        obj.append("2", data1);
//        JSONArray obj3 = (JSONArray) obj.get("1");
//
//        System.out.println(obj3.toString());
        Gson gson = new Gson();
       // String data2 = obj.getString("1");
        //Type stringStringMap = new TypeToken<Map<Integer, String>>(){}.getType();
        JSONArray o = (JSONArray) obj.get("1");
        String inString = o.get(0).toString();

        Map<Integer,String> retMap =(Map<Integer,String> ) gson.fromJson(inString, data.getClass());


        Map<Integer,String> retMap1 =(Map<Integer,String> ) gson.fromJson(obj.get("2").toString(), data.getClass());

        System.out.println(retMap);
        System.out.println(retMap1);

    }
}
