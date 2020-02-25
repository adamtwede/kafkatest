import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Properties;

// todo: use type params to do chosen serialization/deserialization
public class ItemStream<PT, CT> implements Runnable {

    volatile boolean stop = false;

    private final Properties streamsConfiguration;
    // Set up serializers and deserializers, which we will use for overriding the default serdes
    // specified above.
    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
    private final Serde<SourceItem> sourceItemSerde = Serdes.serdeFrom(new SourceItemSerializer(), new SourceItemDeserializer());
    private final Serde<DestinationItem> destinationItemSerde =
            Serdes.serdeFrom(new DestinationItemSerializer(), new DestinationItemDeserializer());

    // In the subsequent lines we define the processing topology of the Streams application.
    private final StreamsBuilder builder = new StreamsBuilder();

    private final Class producerType;
    private final Class consumerType;

    public ItemStream(Class producerType, Class consumerType) {
        this.producerType = producerType;
        this.consumerType = consumerType;

        streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-lambda-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        System.out.println("stream ready to process");
    }

    @Override
    public void run() {
        runStream();
    }

    private void runStream() {

        // Read the input Kafka topic into a KStream instance.
        if(producerType.equals(SourceItem.class)) {
            final KStream<byte[], SourceItem> sourceItemKStream = builder.stream(Constants.TOPIC_NAME_FOR_STREAM,
                    Consumed.with(byteArraySerde, sourceItemSerde));
            Gson gson = new Gson();

            // Variant 2: using `map`, modify value only (equivalent to variant 1)
            if(consumerType.equals(DestinationItem.class)) {
                final KStream<String, DestinationItem> mapped = sourceItemKStream
                        .filter((key, value) -> value.getPayload().contains(Constants.TARGET_MESSAGE_SEQUENCE))
                        .map((key, value) -> {
                            Map<String, String> payloadMap = gson.fromJson(value.getPayload(), Map.class); // don't trust this.
                            DestinationItem destinationItem = new DestinationItem();
                            destinationItem.setTargetField(payloadMap.get(Constants.TARGET_MESSAGE_SEQUENCE));
                            destinationItem.setPayload(value.getPayload());
                            //System.out.println("stream value: " + value);
                            return new KeyValue<>(destinationItem.getTargetField(), destinationItem);
                        });
                mapped.to(Constants.TOPIC_NAME_FOR_CONSUMER, Produced.with(stringSerde, destinationItemSerde)); // publish to topic
            }

//        mapped.to(Constants.TOPIC_NAME_FOR_CONSUMER); // publish to topic
        } else if(producerType.equals(String.class)) {
            final KStream<byte[], String> textLines = builder.stream(Constants.TOPIC_NAME_FOR_STREAM,
                    Consumed.with(byteArraySerde, stringSerde));

            final KStream<byte[], String> mapped = textLines
                    .filter((key, value) -> value.contains(Constants.TARGET_MESSAGE_SEQUENCE))
                    .map((key, value) -> new KeyValue<>(key, value.toUpperCase()));

            mapped.to(Constants.TOPIC_NAME_FOR_CONSUMER, Produced.with(byteArraySerde, stringSerde));
        }

        // Variant 1: using `mapValues`
//        final KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(v -> v.toUpperCase());

        // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
        //
        // In this case we can rely on the default serializers for keys and values because their data
        // types did not change, i.e. we only need to provide the name of the output topic.
//        uppercasedWithMapValues.to("UppercasedTextLinesTopic");


        // Variant 3: using `map`, modify both key and value
        //
        // Note: Whether, in general, you should follow this artificial example and store the original
        //       value in the key field is debatable and depends on your use case.  If in doubt, don't
        //       do it.
//        final KStream<String, String> originalAndUppercased = textLines.map((key, value) ->
//                KeyValue.pair(value, value.toUpperCase()));

        // Write the results to a new Kafka topic "OriginalAndUppercasedTopic".
        //
        // In this case we must explicitly set the correct serializers because the default serializers
        // (cf. streaming configuration) do not match the type of this particular KStream instance.
//        originalAndUppercased.to("OriginalAndUppercasedTopic", Produced.with(stringSerde, stringSerde));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the
        // example when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch,
        // which will take time and will require reading all the state-relevant data from the Kafka cluster over the
        // network. Thus in a production scenario you typically do not want to clean up always as we do here but
        // rather only when it is truly needed, i.e., only under certain conditions (e.g., the presence of a command
        // line flag for your app).
        //
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
