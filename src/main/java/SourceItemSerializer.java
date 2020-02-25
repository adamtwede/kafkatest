import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SourceItemSerializer implements Serializer<SourceItem> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, SourceItem sourceItem) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(sourceItem).getBytes();
        } catch (Exception ex) {
            System.out.println("Error serializing object: " + ex.getMessage());
        }
        return retVal;
//        return new byte[0];
    }

//    @Override
//    public byte[] serialize(String topic, Headers headers, SourceItem data) {
//        return new byte[0];
//    }

    @Override
    public void close() {

    }
}
