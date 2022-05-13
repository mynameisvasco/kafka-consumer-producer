package as.proj2.sensor.Entities;

import as.proj2.sensor.Enums.UseCases;
import as.proj2.sensor.Models.SensorData;
import as.proj2.sensor.PConsumer;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class TConsumer extends Thread {
    private final int id;
    private final int group;
    private final UseCases useCase;
    private final Consumer<String, byte[]> consumer;
    private double minTemperature = Double.MAX_VALUE;
    private double maxTemperature = Double.MIN_VALUE;
    private double sum = 0;
    private HashMap<Integer, Integer> totalRecordsByConsumerId = new HashMap<>();
    private PConsumer gui;

    public TConsumer(int id, UseCases useCase, PConsumer gui) {
        this.group = id % useCase.getKafkaConsumerGroupsCount();
        this.useCase = useCase;
        this.id = id;
        this.gui = gui;
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, String.format("Sensor-Group-%d", group));
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");

        if (useCase == UseCases.Uc2 || useCase == UseCases.Uc5) {
            properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        }


        for (int consumerId = 0; consumerId < 6; consumerId++) {
            totalRecordsByConsumerId.put(consumerId, 0);
        }

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("Sensor"));
    }

    public double getMinTemperature() {
        return minTemperature;
    }

    public double getMaxTemperature() {
        return maxTemperature;
    }

    public double getSum() {
        return sum;
    }

    public double getAverage() {
        return sum / totalRecordsByConsumerId.values().stream().reduce(0, Integer::sum);
    }

    @Override
    public void run() {
        System.out.printf("Group %d - Consumer %1d started\n", group, id);
        SensorData lastData = null;
        var exit = false;
        var datas = new LinkedList<SensorData>();

        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));

            for (var record : records) {
                var data = (SensorData) SerializationUtils.deserialize(record.value());

                if (data.temperature() == 0d && data.timestamp() == 0) {
                    System.out.printf("Group %d - Consumer %d: Exit signal \n", group, id);
                    exit = true;
                } else if (useCase == UseCases.Uc1) {
                    handleUc1(data, lastData);
                } else if (useCase == UseCases.Uc2) {
                    handleUc2(data, lastData);
                } else if (useCase == UseCases.Uc5) {
                    handleUc5(data);
                } else if (useCase == UseCases.Uc6) {
                    handleUc6(data);
                }

                totalRecordsByConsumerId.put(data.sensorId() - 1, totalRecordsByConsumerId.get(data.sensorId() - 1) + 1);
                lastData = data;
            }

            gui.addRecords(id, datas);
            consumer.commitAsync();
            if (exit) break;
        }

        for (int consumerId = 0; consumerId < 6; consumerId++) {
            var records = totalRecordsByConsumerId.get(consumerId);

            if (records != 0) {
                gui.setTotalRecords(consumerId, records);
            }
        }
    }

    private void handleUc1(SensorData data, SensorData lastData) {
        if (lastData != null && data.timestamp() < lastData.timestamp()) {
            System.out.printf("Error! Consumer %d: Use case 1 order is incorrect, %s - %s\n", id, lastData, data);
            System.exit(-1);
        }
    }

    private void handleUc2(SensorData data, SensorData lastData) {
        if (data.sensorId() + 1 != id) {
            System.out.printf("Error! Consumer %d: Use case 2 consumer should only read from 1 id, %s - %s\n", id, lastData, data);
            System.exit(-1);
        }

        if (lastData != null && data.timestamp() < lastData.timestamp()) {
            System.out.printf("Error! Consumer %d: Use case 2 order is incorrect, %s - %s\n", id, lastData, data);
            System.exit(-1);
        }
    }

    private void handleUc5(SensorData data) {
        if (data.temperature() > maxTemperature) {
            maxTemperature = data.temperature();
        }

        if (data.temperature() < minTemperature) {
            minTemperature = data.temperature();
        }
    }

    private void handleUc6(SensorData data) {
        sum += data.temperature();
    }
}
