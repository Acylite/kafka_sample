package sample.data.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

public class ReadPropertiesFile {

  private static final Logger LOG = LoggerFactory.getLogger(ReadPropertiesFile.class);
  Properties properties = new Properties();

  public static class Builder {
    private final String env;
    private KafkaStreamingOptions options;
    private final ReadPropertiesFile propertiesFile;

    public Builder(String env) {
      this.env = env;
      this.propertiesFile = new ReadPropertiesFile();
    }

    public Builder addOptions(KafkaStreamingOptions options) {
      this.options = options;
      return this;
    }

    public HashMap<String, Object> build() throws IOException {
      HashMap<String, Object> props = propertiesFile.getPropValues(env);
      if (options == null) {
        return props;
      }
      propertiesFile.addOptions(props, options);
      return props;
    }
  }
  private HashMap<String, Object> getPropHashMap(Properties props, String env) {
    HashMap<String, Object> configMap = new HashMap<>();
    Set<String> keys = props.stringPropertyNames();
    for (String key : keys) {
      configMap.put(key, props.getProperty(key));
    }
    configMap.put("env", env);
    return configMap;
  }

  public HashMap<String, Object> getPropValues(String env) throws IOException {
    SecretHandler handler = new SecretHandler();
    String secretName = "kafka_consumer_properties_" + handler.getSecretEnv(env);
    byte[] props = handler.getSecretBytes(handler.getSecretProject(env), secretName);

    properties.load(new ByteArrayInputStream(props));

    return getPropHashMap(properties, env);
  }

  public void addOptions(HashMap<String, Object> configMap, KafkaStreamingOptions options) {
    if (options != null) {
      configMap.put("schema.registry.url", options.getSchemaRegistry());
      configMap.put("group.id", options.getGroupId());

      for (String key : configMap.keySet()) {
        if (key.equals("group.id")) {
          try {
              LOG.info("group.id: ".concat(configMap.get("group.id").toString()));
          } catch (NullPointerException e) {
              // Handle the case where groupId is null
              System.out.println("Group ID is null");
          } catch (Exception e) {
              // Handle other potential exceptions
              System.out.println("An error occurred: " + e.getMessage());
          }
        }
        if (key.equals("auto.offset.reset")) {
          LOG.info("auto.offset.reset: ".concat(configMap.get("auto.offset.reset").toString()));
        }
      }
    }
  }

}
