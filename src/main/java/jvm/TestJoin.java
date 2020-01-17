package jvm;


import org.apache.commons.collections.map.HashedMap;

import java.util.Map;
import java.util.Set;

public class TestJoin {

    static class Reordering {
        int x = 0, y = 0;

        public void writer() {
            x = 1;
            y = 2;
            //System.out.println(x + "," + y);
        }

        public void reader() {
            int r1 = y;
            int r2 = x;
        }
    }
    //子线程指的是发生在Thread内部的代码，主线程指的是发生在main函数中的代码，我们可以在main函数中通过join()方法让主线程阻塞等待以达到指定顺序执行的目的。
    public static void joinTest() throws InterruptedException {
        int error = 0;
        for (int i = 0; i < 10000; i++) {
            Reordering r = new Reordering();
            Thread t1 = new Thread(() -> r.writer());
            t1.start();
            t1.join();
            new Thread(() -> r.reader()).start();
            if (r.x == 0 || r.y == 0) {
                error++;
            }
        }
        System.out.println(error);
        Thread.sleep(10000);
    }

    public static void main(String[] args) throws InterruptedException {
        //joinTest();
        Map<String, Integer> map = new HashedMap();
        map.put("1", 2);
        map.put("2", 3);
        Set<String> keys =map.keySet();
        map.values();
        keys.forEach(x -> System.out.println(x + "," +map.get(x)));
    }
}
