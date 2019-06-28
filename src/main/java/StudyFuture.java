import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class StudyFuture {

  private static final FutureTask<Integer> future = new FutureTask<>(new CallableTest());

  public static void main(String[] args) {
    final Thread thread = new Thread(future);
    thread.start();
    try {
      Thread.sleep(10000);
      System.out.println("Main thread is running");
      System.out.println("compute success" + future.get());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  static class CallableTest implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
      System.out.println("hi");
      Thread.sleep(10000);
      for (int i = 0; i < 100; i++) {
        System.out.println(i);
      }

      return 0;
    }
  }
}
