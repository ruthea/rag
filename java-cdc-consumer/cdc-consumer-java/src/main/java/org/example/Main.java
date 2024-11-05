package org.example;

import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.RawChangeConsumerProvider;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import okhttp3.*;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.print("Provide scylla password as command line argument");

        Properties prop = new Properties();
        try {
            //load a properties file from class path, inside static method
            prop.load(Main.class.getClassLoader().getResourceAsStream("application.properties"));
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

        String source = prop.getProperty("source");
        String keyspace = prop.getProperty("keyspace");
        String table = prop.getProperty("table");
        String scylla_user= prop.getProperty("scylla_user");
        String password= args[0];

        RawChangeConsumerProvider changeConsumerProvider = threadId -> {
            RawChangeConsumer changeConsumer = change -> {
                System.out.println("recd a change");
                System.out.println(change);
                System.out.println("\nOperation type is " + change.getOperationType());
                System.out.println("\ncells: " + change.getCell("id").getInt() + change.getCell("plot").getString());


                try {
                    callEmbeddingService(change.getCell("id").getInt(), change.getCell("plot").getString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return CompletableFuture.completedFuture(null);
            };
            return changeConsumer;
        };

        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint(source)
                .addTable(new TableName(keyspace, table))
                .withCredentials(scylla_user, password)
                .withConsumerProvider(changeConsumerProvider)
                .withWorkersCount(1)
                .build()) {

            // Start a consumer. You can stop it by using .stop() method
            // or it can be automatically stopped when created in a
            // try-with-resources (as shown above).
            System.out.println("Starting the consumer");
            consumer.validate();
            consumer.start();
            System.out.println("Main thread sleeps for a day");
            Thread.sleep(86400000);


        } catch (Exception ex) {
            System.err.println("Exception occurred while running the Printer: "
                    + ex.getMessage());
        }
    }

    private static void callEmbeddingService(Integer id, String plot) throws Exception {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\r\n    \"id\" : " + id + ",\r\n    \"plot\" : \"" +
                plot + "\"\r\n}");
        Request request = new Request.Builder()
                .url("http://127.0.0.1:5000/embed/movie")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();
    }
}