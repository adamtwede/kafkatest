
public class Constants {
    //public static final String KAFKA_BROKERS = "localhost:9092";
//    public static final String KAFKA_BROKERS = "0.0.0.0:9092";
    public static final String KAFKA_BROKERS = "127.0.0.1:9092";
    public static final Integer MESSAGE_COUNT = 1000;
    public static final String CLIENT_ID = "client1";
    public static final String TOPIC_NAME_FOR_STREAM = "test";
    public static final String TOPIC_NAME_FOR_CONSUMER = "testTransformed";
//    public static final String TOPIC_NAME_FOR_CONSUMER = "test";
    public static final String GROUP_ID_CONFIG = "consumerGroup1";
    public static final Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static final String OFFSET_RESET_LATEST = "latest";
    public static final String OFFSET_RESET_EARLIER = "earliest";
    public static final Integer MAX_POLL_RECORDS = 1;
    public static final String TARGET_MESSAGE_SEQUENCE = "targetSequence";
    public static final int TARGET_MESSAGE_PERCENT = 10; // percent, but 100 won't work


}