package servermess;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import servermess.KafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Messagereceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(Messagereceiver.class);

    private static final int GIVE_UP = 100;

    /*public void consumeMessageClient(String nickname){


        int recordsCount = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                recordsCount++;
                if (recordsCount > GIVE_UP) {
                    break;
                } else {
                    continue;
                }
            }
            for (ConsumerRecord<Long, String> record : consumerRecords)
            {

            }

            /*consumerRecords.forEach(consumerRecord -> LOGGER.info("Consumer Record:({} {})",
                    consumerRecord.key(), consumerRecord.value(),
                    consumerRecord.partition(), consumerRecord.offset()));

            consumer.commitAsync();
        }

        consumer.close();
        LOGGER.info("Done");
    }

    public void consumeMessage(){

        User user = new User();
        final Consumer<Long, String> consumer = KafkaConfig.getConsumer(user.getUserId());

        int recordsCount = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                recordsCount++;
                if (recordsCount > GIVE_UP) {
                    break;
                } else {
                    continue;
                }
            }

            consumerRecords.forEach(consumerRecord -> LOGGER.info("Consumer Record:({} {})",
                    consumerRecord.key(), consumerRecord.value(),
                    consumerRecord.partition(), consumerRecord.offset()));

            consumer.commitAsync();
        }

        consumer.close();
        LOGGER.info("Done");
    }

    public void sendMessage(int numberOfMessageToSend) {
        final Producer<Long, String> kafkaProducer = KafkaConfig.getProducer();

        for (int i = 0; i < numberOfMessageToSend; i++) {

            String message = "Message-" + i;
            ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConstants.TOPIC, 1L, message);

            LOGGER.info("Sending kafka record: {}", record);

            kafkaProducer.send(record);
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }*/

}
