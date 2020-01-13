import java.util.HashMap;

public class ThinkHashMap {


    private static HashMap<Integer, String> map = new HashMap<Integer, String>(2, 0.75f);

    public static void main(String[] args) throws InterruptedException {
        map.put(5, "C");

        map.forEach((k, v) -> { System.out.println(k); System.out.println(v);});
        new Thread("Thread1") {
            public void run() {
                map.put(7, "B");
                map.put(3, "d");
                System.out.println(map);
            }

        }.start();
        new Thread("Thread2") {
            public void run() {
                map.put(3, "A");
                System.out.println(map);
            }

        }.start();
        Thread.sleep(1000);
        System.out.println(map);
    }

}
