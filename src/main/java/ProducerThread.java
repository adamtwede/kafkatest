import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class ProducerThread<T> implements Runnable {

    private volatile boolean stop = false;

    private final Producer<String, T> producer;
    private final int messageCount;
    private final int frequency;
    private final Class type;
//    private boolean useStrings;

    public ProducerThread(Producer<String, T> producer, int msgCount, int freq, Class type) {
        this.producer = producer;
        this.messageCount = msgCount < 0 ? -1 : msgCount;
        this.frequency = freq < 1 ?  -1 : freq;
        this.type = type;
        //this.useStrings = useStrings;
    }

    @Override
    public void run() {
        generateMessages(this.producer, type, messageCount, frequency, Constants.TARGET_MESSAGE_PERCENT);
    }

    private void generateMessages(Producer<String, T> producer, Class type,
                                         long messageCount, int frequency, int targetRate) {
        if(frequency == -1) {
            System.out.println("will generate messages at random (high) frequencies");
        } else {
            System.out.println("will generate one every " + frequency + " ms");
        }
        int targetCount = 0;

        if(messageCount == -1) {
            messageCount = 9999999999L;
            System.out.println("sending messages (nearly) indefinitely, will wait on external timeout to stop.");
        } else {
            System.out.println("sending " + messageCount + " messages");
        }

        int actualMsgCount = 0;
        for(int i = 0; i < messageCount && !stop; i++) {
            if(type.equals(SourceItem.class)) {
                SourceItem message = generateSourceItemMessage(targetRate);
                if(message.getPayload().contains(Constants.TARGET_MESSAGE_SEQUENCE)) {
                    targetCount++;
                }
                producer.send(new ProducerRecord<>(Constants.TOPIC_NAME_FOR_STREAM, RandomStringUtils.randomNumeric(5) + "", (T) message));
            } else {
                String message = generateStringMessage(targetRate);
                if (message.contains(Constants.TARGET_MESSAGE_SEQUENCE)) {
                    targetCount++;
                }
                //Future<RecordMetadata> f =
                producer.send(new ProducerRecord<>(Constants.TOPIC_NAME_FOR_STREAM, RandomStringUtils.randomNumeric(5) + "", (T) message));
            }
            try {
                Thread.sleep(getFrequency(frequency));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            actualMsgCount++;
        }
        if(stop) {
            System.out.println("producer received external stop signal");
        }
        System.out.println("producer generated " + targetCount + " target messages out of " + actualMsgCount);
        producer.close();
    }

    private int getFrequency(int freq) {
        if(freq == -1) {
            Random r = new Random(System.currentTimeMillis());
            return (r.nextInt(100) + 1); // allow max of <bound> ms between messages (avg will be <bound>/2)
        }
        return freq;
    }

    private SourceItem generateSourceItemMessage(int targetRate) {
        if(targetRate > 100 || targetRate < 1) {
            targetRate = 1; // 1%
        }
        Random r = new Random(System.currentTimeMillis());

        SourceItem sourceItem = new SourceItem();

        int randomInt = (r.nextInt(100) + 1);
        sourceItem.setSomeIntField(randomInt);
        sourceItem.setSomeStringField1(RandomStringUtils.randomAlphabetic(5));
        sourceItem.setSomeStringField2(RandomStringUtils.randomAlphabetic(5));

        if(randomInt <= (100 - targetRate)) {
            sourceItem.setPayload("{" +
                    "\"irrelevant\": 123," +
                    "\"somethingElse\": \"test\"," +
                    "}");
        } else {
            sourceItem.setPayload("{" +
                    "\"irrelevant\": 123," +
                    "\"somethingElse\": \"test\"," +
                    "\""+Constants.TARGET_MESSAGE_SEQUENCE+"\": \""+RandomStringUtils.randomAlphanumeric(7)+"\"" +
                    "}");
        }
        return sourceItem;
    }

    private String generateStringMessage(int targetRate) {
        if(targetRate > 100 || targetRate < 1) {
            targetRate = 1; // 1%
        }
        Random r = new Random(System.currentTimeMillis());

        if((r.nextInt(100) + 1) <= (100 - targetRate)) {
            return RandomStringUtils.randomAlphabetic(5);
        } else {
            return Constants.TARGET_MESSAGE_SEQUENCE;
        }
    }

    public void stopProducer() {
        stop = true;
    }
}
