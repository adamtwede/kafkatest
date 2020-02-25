import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class DestinationItemDeserializer implements Deserializer<DestinationItem> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public DestinationItem deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        DestinationItem destinationItem = null;
        try {
            destinationItem = mapper.readValue(bytes, DestinationItem.class);
        } catch (Exception ex) {
            System.out.println("Error deserializing object: " + ex.getMessage());
//            ex.printStackTrace();
        }
        return destinationItem;
    }

    @Override
    public void close() {

    }
}
