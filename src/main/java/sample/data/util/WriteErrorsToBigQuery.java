package sample.data.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Date;

public class WriteErrorsToBigQuery extends PTransform<PCollection<ProcessingError>, WriteResult> {
  private final String project;
  private final String dataset;

  public WriteErrorsToBigQuery(String project, String dataset) {
    this.project = project;
    this.dataset = dataset;
  }

  public static TableSchema tableSchema = new TableSchema().setFields(
          ImmutableList.of(
                  new TableFieldSchema()
                          .setName("topic").setMode("NULLABLE").setType("STRING"),
                  new TableFieldSchema()
                          .setName("timestamp").setMode("NULLABLE").setType("TIMESTAMP"),
                  new TableFieldSchema()
                          .setName("messageId").setMode("NULLABLE").setType("STRING"),
                  new TableFieldSchema()
                          .setName("errorReason").setMode("NULLABLE").setType("STRING"),
                  new TableFieldSchema()
                          .setName("element").setMode("NULLABLE").setType("STRING"),
                  new TableFieldSchema()
                          .setName("errorTimestamp").setMode("REQUIRED").setType("TIMESTAMP")
          )
  );

  @Override
  public WriteResult expand(PCollection<ProcessingError> errors) {
    String destinationTable = project.concat(":").concat(dataset).concat(".errors");
    return errors
      .apply("Convert Error to TableRow",
          ParDo.of(new Converter())
      )
      .apply("Write Errors to BQ",
          BigQueryIO.writeTableRows()
                  .to(destinationTable)
                  .withSchema(tableSchema)
                  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                  .withExtendedErrorInfo()
      );
  }

  private static class Converter extends DoFn<ProcessingError, TableRow> {
    @ProcessElement
    public void processElement(@Element ProcessingError error, OutputReceiver<TableRow> receiver) {
      TableRow row = new TableRow();
      row.set("topic", error.topic);
      row.set("timestamp", error.timestamp.getTime() / 1000);
      row.set("messageId", error.messageId);
      row.set("errorReason", error.errorReason);
      row.set("element", error.element);
      row.set("errorTimestamp", new Date().getTime() / 1000);
      receiver.output(row);
    }
  }
}
