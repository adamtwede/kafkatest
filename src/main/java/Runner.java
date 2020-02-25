import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public class Runner<PT, CT> {
    private final Producer<String, PT> producer;
    private final Consumer<String, CT> consumer;
    private final int producerHeadstart;
    private final int timeoutMs;
    private final int messageCount;
    private final int frequency;
    private final Class producerType;
    private final Class consumerType;

    //ProducerThread<T> producerThread;
    //ConsumerThread<T> consumerThread;


    Runner(Producer<String, PT> prod, Consumer<String, CT> cons, Class producerType, Class consumerType,
           int producerHeadstart, int timeoutMs,
           int msgCount, int freq) {
        this.producer = prod;
        this.consumer = cons;
        this.producerHeadstart = producerHeadstart;
        this.timeoutMs = timeoutMs;
        this.producerType = producerType;
        this.consumerType = consumerType;
        this.messageCount = msgCount;
        this.frequency = freq;
    }

    public void go() {
        ProducerThread<PT> pRunnable = new ProducerThread<>(producer, messageCount, frequency, producerType);
        Thread pThread = new Thread(pRunnable);
        pThread.start();

        try {
            Thread.sleep(producerHeadstart);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ItemStream<PT, CT> itemStreamRunnable = new ItemStream<>(producerType, consumerType);
        Thread sThread = new Thread(itemStreamRunnable);
        sThread.start();

        ConsumerThread<CT> cRunnable = new ConsumerThread<>(consumer);
        Thread cThread = new Thread(cRunnable);
        cThread.start();

        long startTime = System.currentTimeMillis();

        while(true) {
            if(System.currentTimeMillis() - startTime > timeoutMs) {
                pRunnable.stopProducer();
//                    itemStreamRunnable.stopStream();
//                    Thread.sleep(200);
//                    System.out.println("");
                break;
            } else if(!pThread.isAlive()) {
                break;
            }
        }
    }

}