package org.example;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.errorprone.annotations.CheckReturnValue;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;


/** */
@CheckReturnValue // see go/why-crv
final public class ReadData {
  private static final String COLUMN_FAMILY = "data";
  private static final String COLUMN_QUALIFIER_VALUE = "value";
  private static final String ROW_KEY_PREFIX = "rowKey";

  private ReadData() {}

  public static BigtableDataClient createDataClient(
      String projectId, String instanceId, String appProfileId,
      String dataEndpoint) throws IOException {
    BigtableDataSettings.Builder settingsBuilder =
        BigtableDataSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId);
    if (!appProfileId.isEmpty()) {
      settingsBuilder.setAppProfileId(appProfileId);
    }
    if (!dataEndpoint.isEmpty()) {
      settingsBuilder.stubSettings().setEndpoint(dataEndpoint).build();
    }
    return BigtableDataClient.create(settingsBuilder.build());
  }

  private static void readRow(BigtableDataClient dataClient, String tableId, String rowKey) {
    try {
      Row row = dataClient.readRow(tableId, rowKey);
      if (row == null) {
        System.out.println(
            LocalDateTime.now().toString() + " - Could not read row with key "
                + rowKey);
        return;
      }
      for (RowCell cell : row.getCells()) {
        System.out.printf(
            "%s - Row: %s - Family: %s    Qualifier: %s    Value: %s%n",
            LocalDateTime.now(),
            row.getKey().toStringUtf8(),
            cell.getFamily(),
            cell.getQualifier().toStringUtf8(),
            cell.getValue().toStringUtf8());
      }
    } catch (NotFoundException e) {
      System.out.println("Failed to read from a non-existent table: "
          + e.getMessage());
    } catch (Exception e) {
      System.out.println("Failed to read for some reason: " + e.getMessage());
    }
  }

  public static void main(String[] args) throws IOException {
    String projectId = "google.com:cloud-bigtable-dev";

    //String dataEndpoint = "bigtable.googleapis.com:443";
    //String dataEndpoint = "directpath-bigtable.googleapis.com:443";
    //String instanceId = "guidou-instance-1"; // prod

    String dataEndpoint = "test-bigtable.sandbox.googleapis.com:443";
    String instanceId = "guidou-test-2"; // test

    //String dataEndpoint = "staging-bigtable.sandbox.googleapis.com:443";

    String appProfileId = "mc";
    //String appProfileId = "single";
    //String appProfileId = "default";

    String tableId = "guidou-table-1";

    // Creates a bigtable data client.
    System.out.println("CONNECTING TO " + dataEndpoint);
    BigtableDataClient dataClient =
        createDataClient(projectId, instanceId, appProfileId, dataEndpoint);
    System.out.printf(
        "%s - Client ready",
        LocalDateTime.now());
    System.out.println("Going to read data!");
    final int N = 1;
    for (int i = 0; i < N; i++) {
      readRow(dataClient, tableId, ROW_KEY_PREFIX + i);
    }
  }
}
