/* using the okhttp package 
install from maven https://mvnrepository.com/artifact/com.squareup.okhttp3/okhttp */
import okhttp3.*;
import java.io.IOException;

public class SendRequest {
  public static void main(String[] args) {

    OkHttpClient client = new OkHttpClient();
    MediaType mediaType = MediaType.parse("application/json");

    RequestBody body = RequestBody.create(mediaType, "" +
        "{\r\n\t\"appsflyer_id\": \"1558181549261-2472223433442381156\"," +
        "\r\n\t\"customer_user_id\": \"123456\",\r\n\t\"eventName\": \"af_purchase\",\r\n\t\"" +
        "eventValue\": \"{\\\"af_revenue\\\":\\\"6\\\" ,\\\"af_content_id\\\":\\\"15854\\\"}\",\r\n\t\"" +
        "eventCurrency\": \"USD\",\r\n\t\"" +
        "eventTime\": \"2019-06-11 02:17:00.000\",\r\n\t\"" +
        "af_events_api\" :\"true\"\r\n}");

    Request request = new Request.Builder()
        .url("https://api2.appsflyer.com/inappevent/id1358582620")
        .post(body)
        .addHeader("Content-Type", "application/json")
        .addHeader("authentication", "xL72jaRySLxjKCvsrDvVU5")
        .build();

    try {
      Response response = client.newCall(request).execute();
      System.out.println(response.code());
      System.out.println(response.body().string());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}