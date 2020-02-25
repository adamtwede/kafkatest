import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class SourceItemDeserializer implements Deserializer<SourceItem> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SourceItem deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        SourceItem sourceItem = null;
        try {
            sourceItem = mapper.readValue(bytes, SourceItem.class);
        } catch (Exception ex) {
            System.out.println("Error deserializing object: " + ex.getMessage());
//            ex.printStackTrace();
        }
        return sourceItem;
    }

//    @Override
//    public SourceItem deserialize(String topic, Headers headers, byte[] data) {
//        return null;
//    }

    @Override
    public void close() {

    }
}
