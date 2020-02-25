import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class DestinationItemSerializer implements Serializer<DestinationItem> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, DestinationItem destinationItem) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(destinationItem).getBytes();
        } catch (Exception ex) {
            System.out.println("Error serializing object: " + ex.getMessage());
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
