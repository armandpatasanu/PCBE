package servermess;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class MsgReceiver extends Thread {

    private final Consumer<Long, String> consumer;
    private static final Logger LOGGER = LoggerFactory.getLogger(Messagereceiver.class);

    private static final int GIVE_UP = 100;

    public MsgReceiver(Consumer<Long, String> consumer, String topic) {

        this.consumer = consumer;
        consumer.subscribe(Collections.singleton(topic));
        System.out.println("Initialised mess receiver" + topic);
    }

    public void run() {
        int recordsCount = 0;
       /* while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() == 0) {
                recordsCount++;
                if (recordsCount > GIVE_UP) {
                    break;
                } else {
                    continue;
                }
            }
        }*/
        try {
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Long, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        } finally {

           /* for (ConsumerRecord<Long, String> record : consumerRecords) {
                    System.out.println(record.value());
                }*/

            consumer.close();
        }
            /*consumerRecords.forEach(consumerRecord -> LOGGER.info("Consumer Record:({} {})",
                    consumerRecord.key(), consumerRecord.value(),
                    consumerRecord.partition(), consumerRecord.offset()));

            consumer.commitAsync();
        }

        consumer.close();
        LOGGER.info("Done");*/
    }

}


