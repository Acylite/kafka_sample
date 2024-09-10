package sample.data.util;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaConsumerFactoryFn implements
    SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerFactoryFn.class);
  private String env;

  public String getEnv(Map<String, Object> config) {
    if (config.get("env") == null) {
      LOG.error(
              "env param was not set. Please add props.put(\"env\", env); when setting up your pipeline");
      System.exit(1);
    } else {
      env = config.get("env").toString();
    }

    return env;
  }


  @Override
  public Consumer<byte[], byte[]> apply(Map<String, Object> config) {

    // TODO copy files to local machine as part of GHA instead of using this
    // This currently prevents me from running dataflows locally

    String env = getEnv(config);

    new sample.data.util.SecretHandler().loadSecrets(env);

    return new KafkaConsumer<>(config);
    }
  }
