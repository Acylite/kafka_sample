package sample.data.util;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface KafkaStreamingOptions extends PipelineOptions {
  /**
   * By default, this example uses Playground's Kafka server. Set this option to different value
   * to use your own Kafka server.
   */
  @Description("Kafka Bootstrap Servers")
  String getBootstrapServers();
  void setBootstrapServers(String bootstrapServers);

  @Description("Kafka Schema Registry")
  String getSchemaRegistry();
  void setSchemaRegistry(String schemaRegistry);

  String getGroupId();
  void setGroupId(String groupId);

  String getEnv();
  void setEnv(String env);

  String getTopic();
  void setTopic(String topic);

  String getPubsubTopic();

  void setPubsubTopic(String value);

  String getRawTableName();
  void setRawTableName(String rawTableName);

  String getParsedTableName();
  void setParsedTableName(String parsedTableName);

  String getDatasetName();
  void setDatasetName(String datasetName);

  String getCollectionId();
  void setCollectionId(String collectionId);

  String getTransformationStrategy();
  void setTransformationStrategy(String value);

  String getFilteringStrategy();
  void setFilteringStrategy(String value);

  String getModelClassName();
  void setModelClassName(String value);
}