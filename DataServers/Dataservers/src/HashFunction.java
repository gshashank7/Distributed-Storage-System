package DataServers.Dataservers.src;

/**
 * Created by shobhitgarg on 4/26/17.
 */
public class HashFunction {

    public static void main(String[] args) {
        System.out.println(getHash("B. Thomas Golisano College of Computing and Information Sciences"));
        System.out.println(getHash("CTRL ALT DELi RIT"));
        System.out.println(getHash("Shobhit"));
        System.out.println(getHash("Nikhilesh"));
        System.out.println(getHash("Shashank"));
    }
    static int getHash(String data) {
        int hash = 0;
        for(int i = 0; i<data.length(); i++)    {
            hash += (int) data.charAt(i);
        }
        return hash % 360;
    }

}
