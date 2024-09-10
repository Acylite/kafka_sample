package sample.data.util;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;


public class WriteToBigQuery extends PTransform<PCollection<TableRow>, WriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger("WriteToBigQueryLogger");

  private final String project;
  private final String dataset;
  private final String table;
  private final TableSchema schema;

  public WriteToBigQuery(String project, String dataset, String table, TableSchema schema) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.schema = schema;
  }

  @Override // Todo - return nothing instead of WriteResult
  public WriteResult expand(PCollection<TableRow> rows) {
    String destinationTable = project.concat(":").concat(dataset).concat(".").concat(table);
    String topic = table.replace("_", "-");

    WriteResult bigQueryRecords = rows.apply("Write to " + destinationTable,
        BigQueryIO.writeTableRows()
            .to(destinationTable)
            .withSchema(schema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withExtendedErrorInfo()
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
    );

    bigQueryRecords.getFailedInsertsWithErr()
        .apply(
            MapElements.into(TypeDescriptor.of(ProcessingError.class))
                .via(err -> {
                      LOG.error("Error: {}.\nRow: {}", err.getError(), err.getRow());
                      String errorReason = err.getError().toString();
                      String element = err.getRow().toString();
                      String messageId = ""; // Not sure how to extract this
                      return new ProcessingError(topic, new Date(), messageId, errorReason, element);
                    }
                )
        )
        .apply("Write Errors to BQ", new WriteErrorsToBigQuery(project, dataset));

    return bigQueryRecords;
  }

}
