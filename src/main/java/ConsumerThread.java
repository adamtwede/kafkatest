import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

public class ConsumerThread<T> implements Runnable {

    private final Consumer<String, T> consumer;

    public ConsumerThread(Consumer<String, T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        System.out.println("about to start consumer");
        startConsumer(this.consumer);
    }

    private void startConsumer(Consumer<String, T> consumer) {
        int total = 0;
        int noMessageFound = 0;
        int targetMessagesFound = 0;

        while (true) {
            ConsumerRecords<String, T> consumerRecords = consumer.poll(Duration.ofMillis(1000)); // param is poll wait time if no messages are found
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > Constants.MAX_NO_MESSAGE_FOUND_COUNT) { // exit poll timeout
                    break;
                }
                continue;
            }

            total += consumerRecords.count();

            for (ConsumerRecord<String, T> record : consumerRecords) {

                if (record.value() instanceof String) {
                    String stringValue = (String)record.value();
    //                System.out.println("from consumer, record: " + record.value());
                    if(stringValue.contains(Constants.TARGET_MESSAGE_SEQUENCE)) {
                        targetMessagesFound++;
    //                    System.out.println("target sequence identified in record " + record.key());
                    }

                } else if(record.value() instanceof SourceItem) { // you'd use this if bypassing the Stream
                    SourceItem sourceItem = (SourceItem) record.value();
                    if(sourceItem.getPayload().contains(Constants.TARGET_MESSAGE_SEQUENCE)) {
                        //System.out.println("found source item in topic: " + sourceItem.getPayload());
                        targetMessagesFound++;
                    }
                } else if(record.value() instanceof DestinationItem) { // use this with the Stream (the Stream transforms SourceItem to DestinationItem)
                    DestinationItem destinationItem = (DestinationItem) record.value();
                    if(destinationItem.getPayload().contains(Constants.TARGET_MESSAGE_SEQUENCE)) {
                        //System.out.println("found source item in topic: " + sourceItem.getPayload());
                        targetMessagesFound++;
                    }
                }
            }
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();

        System.out.println("consumer found " + targetMessagesFound + " target messages out of " + total);
    }
}
