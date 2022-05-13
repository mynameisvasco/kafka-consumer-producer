package as.proj2.sensor.Entities;

import as.proj2.sensor.Enums.UseCases;
import as.proj2.sensor.Models.SensorData;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Properties;

public class TProducer extends Thread {
    private final int id;
    private final UseCases useCase;
    private final Producer<String, byte[]> producer;
    private final ObjectInputStream inputStream;

    public TProducer(int id, UseCases useCase, ObjectInputStream inputStream) {
        this.id = id;
        this.useCase = useCase;
        this.inputStream = inputStream;
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "as.proj2.sensor.Models.SensorIdPartitioner");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "100000");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");

        if (useCase == UseCases.Uc1 || useCase == UseCases.Uc5) {
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "0");
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        }

        if (useCase == UseCases.Uc2) {
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        }

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void run() {

        while (true) {
            try {
                var data = (SensorData) inputStream.readObject();
                handleSendUc1(data);

                if (data.temperature() == 0d && data.timestamp() == 0) {
                    break;
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleSendUc1(SensorData data) {
        var key = String.format("%s", (data.sensorId() - 1) % useCase.getKafkaConsumersCount()) ;
        var record = new ProducerRecord<>("Sensor", key, SerializationUtils.serialize(data));
        producer.send(record);
        System.out.format("Producer %d: %s\n", id, data);
    }
}