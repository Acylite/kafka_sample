package sample.data.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class PropertiesLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoader.class);
    public static HashMap<String, Object> loadProperties(String env, KafkaStreamingOptions options) {
        HashMap<String, Object> props;
        try {
            props = new ReadPropertiesFile.Builder(env)
                    .addOptions(options)
                    .build();
        } catch (Exception e) {
            LOG.info("Could not load properties");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
        return props;
    }
}

