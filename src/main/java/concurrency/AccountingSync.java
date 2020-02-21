package concurrency;




public class AccountingSync implements Runnable {

    static int i = 0;


    public synchronized void increase() {
        i++;
    }

    @Override
    public void run() {
        for (int j = 0; j < 1000000; j++) {
            increase();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        AccountingSync instance = new AccountingSync();
        Thread t1 = new Thread(instance);
        Thread t2 = new Thread(instance);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        System.out.println(i);
    }

}

class AccountingSyncTwoInstance implements Runnable {

    static int i = 0;

    public synchronized void increase() {
        i++;
    }

    @Override
    public void run() {
        for (int j = 0; j < 1000000; j++) {
            increase();
        }
    }

    public static void main(String[] args) throws InterruptedException {

        Thread t3 = new Thread(new AccountingSyncTwoInstance());
        Thread t4 = new Thread(new AccountingSyncTwoInstance());
        t3.start();
        t4.start();

        Thread.sleep(32);
        t3.join();
        t4.join();
        System.out.println(i);
    }

}


class AccountingSyncObj implements Runnable {

    static int i = 0;

    Object object = new Object();

    public void increase() {
        synchronized (object) {
            try {
                object.wait(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }

    }

    @Override
    public void run() {
        for (int j = 0; j < 100; j++) {
            increase();
        }
    }

    public static void main(String[] args) throws InterruptedException {

        Thread t4 = new Thread(new AccountingSyncObj());
        t4.start();
        t4.join();
        char a = (char) Integer.MAX_VALUE;
        System.out.println((int) a);
        System.out.println((int) '中');
        System.out.println(i);
    }

}

