### Kakfa Test

This application runs a test of a basic Kafka Producer, Stream, and Consumer. Each runs in its own thread to
simulate asynchronous message generation/filtering/consumption, with the Producer getting a head start. 
The Producer generates a randomized number of "target" messages that the Consumer looks for. When the Stream runs, 
it does the filtering on behalf of the Consumer and publishes only the target messages to a new topic for the 
Consumer. There is an example of basic object serialization and deserialization as well. 

This project is meant to show the basic concepts of a Kafka processing flow for reference and simple tests. 
You can modify the code to send a set number of messages or run for a variable amount of time, as well as 
generate a variable number of target messages (default is 10%, but it won't be exact since they are 
randomized--the more messages you send or the longer it runs the closer to the target percentage you will get).

1. download and unzip Kakfa (https://kafka.apache.org/quickstart)
and follow the instructions, ie:
2. run zookeeper from folder: 

        ~/kafka_2.12-2.4.0/zookeeper-server-start.sh config/zookeeper.properties
        
3. run (single instance) kafka broker:

        ~/kafka_2.12-2.4.0/bin/kafka-server-start.sh config/server.properties
        
4. run this application by running Main class

The message queue persists between Kafka shutdowns as messages are persisted to disk, but you can purge the 
Kafka message queue by setting the message TTL to a small value:

    ~/kafka_2.12-2.4.0/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic test --config retention.ms=1000
    
And then running the same command again with a higher TTL (otherwise your messages will be deleted after
short interval you set in the first command). Be sure to set the topic name from "test" to something else
if you've renamed it.

If you're running the Kafka Streams version (enabled by default) the ItemStream class will publish its transformed
queue to a separate topic called "testTransformed", which the consumer will automatically read from, so you may
also need to purge that topic as well if you want to reset everything.

If you don't shut Zookeeper/Kafka down properly (like if it hangs for some reason and you have to terminate it) 
you may get a Zookeeper error like this:

    java.io.IOException: No snapshot found, but there are log entries. Something is broken!
    
Just delete the /tmp/zookeeper folder to fix it (this folder is set in config/zookeeper.properties and 
you can change it). I assure you I've never had to do this because I always shut things down properly. 

Note that when the Streams version is enabled (again, by default it is) then the application will continue
to run even after messages have stopped being produced and you will need to ctrl+c or stop the process from 
within your IDE. It shuts down gracefully when it receives SIGINT from IDE stop or ctrl+c but programmatically 
shutting it down (by stopping the thread the Stream is running in) causes it to bypass some necessary cleanup 
and shout obscenities, so feel free to fix that if you want. 