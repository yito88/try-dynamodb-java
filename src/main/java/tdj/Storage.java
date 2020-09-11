package tdj;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class Storage {
  private final DynamoDbClient client;

  public Storage(Region region) {
    this.client = DynamoDbClient.builder().region(region).build();
  }

  public void close() {
    client.close();
  }

  public void createTable(String table, String partitionKey, Long throughput)
      throws DynamoDbException {
    // TODO: change the type
    CreateTableRequest request =
        CreateTableRequest.builder()
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(partitionKey)
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(partitionKey)
                    .keyType(KeyType.HASH)
                    .build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(throughput)
                    .writeCapacityUnits(throughput)
                    .build())
            .tableName(table)
            .build();

    client.createTable(request);
  }

  public void createTable(String table, String partitionKey, String sortKey, Long throughput)
      throws DynamoDbException {
    // TODO: change the type
    CreateTableRequest request =
        CreateTableRequest.builder()
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(partitionKey)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(sortKey)
                    .attributeType(ScalarAttributeType.N)
                    .build())
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(partitionKey)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder().attributeName(sortKey).keyType(KeyType.RANGE).build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(throughput)
                    .writeCapacityUnits(throughput)
                    .build())
            .tableName(table)
            .build();

    client.createTable(request);
  }

  public void deleteTable(String table) throws DynamoDbException {
    DeleteTableRequest request = DeleteTableRequest.builder().tableName(table).build();

    client.deleteTable(request);
  }

  public void put(String table, Map<String, AttributeValue> values) throws DynamoDbException {
    PutItemRequest request = PutItemRequest.builder().tableName(table).item(values).build();

    client.putItem(request);
  }

  public void putWithCondition(
      String table,
      Map<String, AttributeValue> values,
      String conditions,
      Map<String, AttributeValue> expressionAttributeValues)
      throws DynamoDbException {
    PutItemRequest request =
        PutItemRequest.builder()
            .tableName(table)
            .item(values)
            .conditionExpression(conditions)
            .expressionAttributeValues(expressionAttributeValues)
            .build();

    client.putItem(request);
  }

  public void update(
      String table, Map<String, AttributeValue> key, Map<String, AttributeValue> values)
      throws DynamoDbException {
    Map<String, AttributeValueUpdate> updateValues = new HashMap<>();
    values.forEach(
        (a, v) -> {
          updateValues.put(
              a, AttributeValueUpdate.builder().value(v).action(AttributeAction.PUT).build());
        });

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName(table)
            .key(key)
            .attributeUpdates(updateValues)
            .build();

    client.updateItem(request);
  }

  public void updateWithCondition(
      String table,
      Map<String, AttributeValue> key,
      Map<String, AttributeValue> values,
      String conditions,
      Map<String, AttributeValue> expressionAttributeValues)
      throws DynamoDbException {
    String updateExpression = "SET ";
    int i = 0;
    for (Map.Entry<String, AttributeValue> entry : values.entrySet()) {
      String a = entry.getKey();
      AttributeValue v = entry.getValue();

      updateExpression += a + "=:v" + String.valueOf(i);
      expressionAttributeValues.put(":v" + String.valueOf(i), v);

      i++;
      if (i < values.size()) {
        updateExpression += ",";
      }
    }

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName(table)
            .key(key)
            .updateExpression(updateExpression)
            .conditionExpression(conditions)
            .expressionAttributeValues(expressionAttributeValues)
            .build();

    client.updateItem(request);
  }

  public Map<String, AttributeValue> get(String table, Map<String, AttributeValue> key)
      throws DynamoDbException {
    GetItemRequest request =
        GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build();

    return client.getItem(request).item();
  }

  public void delete(String table, Map<String, AttributeValue> key) throws DynamoDbException {
    DeleteItemRequest request = DeleteItemRequest.builder().tableName(table).key(key).build();

    client.deleteItem(request);
  }
}
