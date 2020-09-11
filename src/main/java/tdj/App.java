package tdj;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

public class App {
  static final Region REGION = Region.AP_NORTHEAST_1;
  static final int POPULATION_CONCURRENCY = 1;
  static final int NUM_PER_THREAD = 1000;
  static final int CONCURRENCY = 8;
  static final String TABLE = "mytest";
  static final String PARTITION_KEY = "pk";
  static final String SORT_KEY = "sk";
  static final String VALUE1 = "val1";
  static final String VALUE2 = "val2";
  static final String VALUE3 = "val3";
  static final String VALUE4 = "val4";
  static final String TEXT = "mytext";
  static final AtomicInteger count = new AtomicInteger(0);

  public static void main(String[] args) {
    //Storage storage = new Storage(REGION);

    //storage.createTable(TABLE, PARTITION_KEY, SORT_KEY, new Long(100));
    //try {
    //  Thread.sleep(10000);
    //} catch (InterruptedException e) {
    //  Thread.currentThread().interrupt();
    //}

    // crudTest(storage);

    // conditionalInsertionsTest(storage);

    //conditionalUpdatesTest(storage);

    // populateItems(storage);


    // storage.deleteTable(TABLE);

    //storage.close();

    // Type should be get, insert, conditionalInsert, update, conditionalUpdate
    String type = "conditionalUpdate";
    int numItems = 1000;
    int runForSec = 100;
    int concurrency = 8;
    int throughput = 10;
    new Benchmark(type, numItems, runForSec, concurrency, throughput).run();
  }

  private static void crudTest(Storage storage) {
    storage.put(TABLE, makeInitialValues(0));

    Map<String, AttributeValue> key = makeKeyValues(0);
    if (storage.get(TABLE, key) != makeInitialValues(0)) {
      System.err.println("Unexpected result!");
    }

    Map<String, AttributeValue> updateValues = new HashMap<>();
    updateValues.put(VALUE1, AttributeValue.builder().n(String.valueOf(1111)).build());
    updateValues.put(VALUE3, AttributeValue.builder().bool(false).build());
    storage.update(TABLE, key, updateValues);

    Map<String, AttributeValue> newValues = makeInitialValues(0);
    newValues.put(VALUE1, AttributeValue.builder().n(String.valueOf(1111)).build());
    newValues.put(VALUE3, AttributeValue.builder().bool(false).build());
    if (storage.get(TABLE, key) != newValues) {
      System.err.println("Unexpected result!");
    }

    storage.delete(TABLE, key);

    Map<String, AttributeValue> emptyValues = new HashMap<>();
    if (storage.get(TABLE, key) != emptyValues) {
      System.err.println("Unexpected result!");
    }
  }

  private static Map<String, AttributeValue> makeKeyValues(int keyId) {
    Map<String, AttributeValue> values = new HashMap<>();

    String pk = String.valueOf(keyId);
    values.put(PARTITION_KEY, AttributeValue.builder().s(pk).build());
    String sk = String.valueOf(keyId % NUM_PER_THREAD);
    values.put(SORT_KEY, AttributeValue.builder().n(sk).build());

    return values;
  }

  private static Map<String, AttributeValue> makeInitialValues(int keyId) {
    Map<String, AttributeValue> values = makeKeyValues(keyId);

    String val1 = String.valueOf(0);
    values.put(VALUE1, AttributeValue.builder().n(val1).build());

    String val2 = TEXT + String.valueOf(keyId);
    values.put(VALUE2, AttributeValue.builder().s(val2).build());

    boolean val3 = (keyId % 3) == 0;
    values.put(VALUE3, AttributeValue.builder().bool(val3).build());

    byte[] val4 = (TEXT + String.valueOf(keyId)).getBytes(StandardCharsets.UTF_8);
    values.put(VALUE4, AttributeValue.builder().b(SdkBytes.fromByteArray(val4)).build());

    return values;
  }

  private static void conditionalInsertionsTest(Storage storage) {
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
    IntStream.range(0, CONCURRENCY)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> {
                        new InsertionRunner(i, storage).run();
                      },
                      es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();

    if (count.get() == NUM_PER_THREAD) {
      System.out.println("The insertion test worked successfully!");
    } else {
      System.err.println("Count: " + count.get());
      System.err.println("Unexpected result!");
    }
  }

  private static class InsertionRunner {
    private final int threadId;
    private final Storage storage;

    public InsertionRunner(int threadId, Storage storage) {
      this.threadId = threadId;
      this.storage = storage;
    }

    public void run() {
      // multiple threads try to insert items with the same key
      for (int i = 0; i < NUM_PER_THREAD; i++) {
        try {
          String conditions = "NOT " + PARTITION_KEY + " = :pk" + " AND NOT " + SORT_KEY + " = :sk";

          // binding
          Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
          String pk = String.valueOf(i);
          String sk = String.valueOf(i % NUM_PER_THREAD);
          expressionAttributeValues.put(":pk", AttributeValue.builder().n(pk).build());
          expressionAttributeValues.put(":sk", AttributeValue.builder().n(sk).build());

          storage.putWithCondition(
              TABLE, makeInitialValues(i), conditions, expressionAttributeValues);
        } catch (ConditionalCheckFailedException e) {
          System.err.println("Failed due to the conditions");
          continue;
        } catch (Exception e) {
          System.err.println(e.getMessage());
          continue;
        }
        count.getAndIncrement();
      }
    }
  }

  private static void conditionalUpdatesTest(Storage storage) {
    storage.put(TABLE, makeInitialValues(0));

    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
    IntStream.range(0, CONCURRENCY)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> {
                        new UpdateRunner(i, storage).run();
                      },
                      es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();

    Map<String, AttributeValue> key = makeKeyValues(0);
    Map<String, AttributeValue> result = storage.get(TABLE, key);
    if (count.get() == 100 && Integer.valueOf(result.get(VALUE1).n()) == 100) {
      System.out.println("The insertion test worked successfully!");
    } else {
      System.err.println("Count: " + count.get());
      System.err.println("Last value1: " + result.get(VALUE1).n());
      System.err.println("Unexpected result!");
    }
  }

  private static class UpdateRunner {
    private final int threadId;
    private final Storage storage;

    public UpdateRunner(int threadId, Storage storage) {
      this.threadId = threadId;
      this.storage = storage;
    }

    public void run() {
      // multiple threads try to insert items with the same key
      for (int i = 0; i < 100; i++) {
        try {
          String conditions = VALUE1 + " = :c1";
          // binding
          Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
          String val1 = String.valueOf(i);
          expressionAttributeValues.put(":c1", AttributeValue.builder().n(val1).build());

          Map<String, AttributeValue> updateValues = new HashMap<>();
          updateValues.put(VALUE1, AttributeValue.builder().n(String.valueOf(i + 1)).build());

          storage.updateWithCondition(
              TABLE, makeKeyValues(0), updateValues, conditions, expressionAttributeValues);
        } catch (ConditionalCheckFailedException e) {
          System.err.println("Failed due to the conditions");
          continue;
        } catch (Exception e) {
          System.err.println(e.getMessage());
          continue;
        }
        count.getAndIncrement();
      }
    }
  }
}
