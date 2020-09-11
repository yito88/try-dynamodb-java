package tdj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Benchmark {
  private final Region REGION = Region.AP_NORTHEAST_1;
  private final String TABLE = "mybench";
  private final String PARTITION_KEY = "id";
  private final String SORT_KEY = "ver";
  private final String VALUE = "val";

  private final String reqType;
  private final int numItems;
  private final int runForSec;
  private final int concurrency;
  private final int throughput;
  private final Storage storage;
  private final AtomicInteger count = new AtomicInteger(0);

  public Benchmark(String reqType, int numItems, int runForSec, int concurrency, int throughput) {
    this.reqType = reqType;
    this.numItems = numItems;
    this.runForSec = runForSec;
    this.concurrency = concurrency;
    this.throughput = throughput;
    this.storage = new Storage(REGION);
  }

  public void run() {
    prepare();

    measure();
  }

  private void prepare() {
    storage.createTable(TABLE, PARTITION_KEY, SORT_KEY, new Long(throughput));
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (reqType == "get" || reqType == "update" || reqType == "conditionalUpdate") {
      populate();
    }
  }

  private void populate() {
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
    IntStream.range(0, 8)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> {
                        new PopulationRunner(i, storage).run();
                      },
                      es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
  }

  private class PopulationRunner {
    private final int threadId;
    private final Storage storage;

    public PopulationRunner(int threadId, Storage storage) {
      this.threadId = threadId;
      this.storage = storage;
    }

    public void run() {
      // multiple threads try to insert items with the same key
      int numPerThread = numItems / 8;
      for (int i = 0; i < numPerThread; i++) {
        try {
          storage.put(TABLE, makeValues(numPerThread * threadId + i, 0, 0));
        } catch (Exception e) {
          System.err.println(e.getMessage());
          continue;
        }
      }
    }
  }

  private void measure() {
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> {
                        new Runner(i, storage).run();
                      },
                      es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();

    double ops = count.get() / runForSec;
    System.out.println("Throughput " + ops + " ops");
  }

  private class Runner {
    private final int threadId;
    private final Storage storage;

    public Runner(int threadId, Storage storage) {
      this.threadId = threadId;
      this.storage = storage;
    }

    public void run() {
      // multiple threads try to insert items with the same key
      long end = System.currentTimeMillis() + runForSec * 1000;
      while (System.currentTimeMillis() < end) {
        try {
          switch (reqType) {
            case "get":
              int key = ThreadLocalRandom.current().nextInt(numItems);
              storage.get(TABLE, makeKey(key, 0));
              break;

            case "insert":
              storage.put(TABLE, makeValues(ThreadLocalRandom.current().nextInt(), 0, 0));
              break;

            case "conditionalInsert":
              String conditions =
                  "NOT " + PARTITION_KEY + " = :pk" + " AND NOT " + SORT_KEY + " = :sk";
              storage.putWithCondition(
                  TABLE,
                  makeValues(ThreadLocalRandom.current().nextInt(), 0, 0),
                  conditions,
                  bindKeyExpression(ThreadLocalRandom.current().nextInt(), 0));
              break;

            case "update":
              int anyKey = ThreadLocalRandom.current().nextInt(numItems);
              int anyValue = ThreadLocalRandom.current().nextInt();
              storage.update(TABLE, makeKey(anyKey, 0), makeUpdateValues(anyValue));
              break;

            case "conditionalUpdate":
              int anyKey2 = ThreadLocalRandom.current().nextInt(numItems);
              int anyValue2 = ThreadLocalRandom.current().nextInt(10000);
              String conditions2 = VALUE + " >= :c1";
              Map<String, AttributeValue> bind = new HashMap<>();
              bind.put(":c1", AttributeValue.builder().n(String.valueOf(0)).build());
              storage.updateWithCondition(
                  TABLE, makeKey(anyKey2, 0), makeUpdateValues(anyValue2), conditions2, bind);

              break;
          }
        } catch (Exception e) {
          System.err.println(e.getMessage());
          continue;
        }
        count.getAndIncrement();
      }
    }
  }

  private Map<String, AttributeValue> makeKey(int id, int ver) {
    Map<String, AttributeValue> values = new HashMap<>();

    String pk = String.valueOf(id);
    values.put(PARTITION_KEY, AttributeValue.builder().s(pk).build());
    String sk = String.valueOf(ver);
    values.put(SORT_KEY, AttributeValue.builder().n(sk).build());

    return values;
  }

  private Map<String, AttributeValue> makeValues(int id, int ver, int val) {
    Map<String, AttributeValue> values = makeKey(id, ver);

    String value = String.valueOf(val);
    values.put(VALUE, AttributeValue.builder().n(value).build());

    return values;
  }

  private Map<String, AttributeValue> bindKeyExpression(int id, int ver) {
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    String pk = String.valueOf(id);
    String sk = String.valueOf(ver);
    expressionAttributeValues.put(":pk", AttributeValue.builder().n(pk).build());
    expressionAttributeValues.put(":sk", AttributeValue.builder().n(sk).build());

    return expressionAttributeValues;
  }

  private Map<String, AttributeValue> makeUpdateValues(int val) {
    Map<String, AttributeValue> values = new HashMap<>();
    String value = String.valueOf(val);
    values.put(VALUE, AttributeValue.builder().n(value).build());

    return values;
  }
}
