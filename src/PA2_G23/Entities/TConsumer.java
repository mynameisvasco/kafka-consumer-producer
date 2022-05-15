package PA2_G23.Entities;

import PA2_G23.Models.SensorData;
import PA2_G23.Util;
import PA2_G23.Enums.UseCases;
import PA2_G23.PConsumer;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class TConsumer extends Thread {
    private final int id;
    private final int group;
    private final UseCases useCase;
    private final Consumer<String, byte[]> consumer;
    private final PConsumer gui;
    private final int[] totalRecords = {0, 0, 0, 0, 0, 0};
    private final int[] totalSum = {0, 0, 0, 0, 0, 0};
    private final double[] min = {Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE};
    private final double[] max = {Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE};

    public TConsumer(int id, UseCases useCase, PConsumer gui) {
        this.group = id % useCase.getKafkaConsumerGroupsCount();
        this.useCase = useCase;
        this.id = id;
        this.gui = gui;
        var properties = new Properties();
        Util.configureConsumer(useCase, properties);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, String.valueOf(group));
        consumer = new KafkaConsumer<>(properties);
        consumer.assign(List.of(new TopicPartition("Sensor", id % useCase.getKafkaPartitionsCount())));
    }

    @Override
    public void run() {
        System.out.printf("Group %d - Consumer %1d started\n", group, id);
        SensorData lastData = null;
        var exit = false;
        var dataList = new LinkedList<SensorData>();

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
                } else if (useCase == UseCases.Uc4) {
                    handleUc4(data, lastData);
                } else if (useCase == UseCases.Uc5) {
                    handleUc5(data);
                } else if (useCase == UseCases.Uc6) {
                    handleUc6(data);
                }

                if (!exit) {
                    dataList.add(data);
                    totalRecords[data.sensorId() - 1]++;
                }

                if (dataList.size() > 10000) {
                    gui.addRecords(id, new LinkedList<>(dataList));
                    dataList.clear();
                }

                lastData = data;
            }

            consumer.commitAsync();
            if (exit) break;
        }

        for (int i = 0; i < 6; i++) {
            if (totalRecords[i] != 0) {
                if (useCase == UseCases.Uc5) {
                    gui.setTotalRecords(i, totalRecords[i], min[i], max[i]);
                } else if (useCase == UseCases.Uc6) {
                    gui.setTotalRecords(i, totalRecords[i], (double) totalSum[i] / totalRecords[i]);
                } else {
                    gui.setTotalRecords(i, totalRecords[i]);
                }
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
        if (lastData != null && data.sensorId() != lastData.sensorId()) {
            System.out.printf("Error! Consumer %d: Use case 2 consumer should only read from 1 id, %s - %s\n", id, lastData, data);
            System.exit(-1);
        }

        if (lastData != null && data.timestamp() < lastData.timestamp()) {
            System.out.printf("Error! Consumer %d: Use case 2 order is incorrect, %s - %s\n", id, lastData, data);
            System.exit(-1);
        }
    }

    private void handleUc4(SensorData data, SensorData lastData) {
        if (lastData != null && data.timestamp() < lastData.timestamp()) {
            System.out.printf("Error! Consumer %d: Use case 1 order is incorrect, %s - %s\n", id, lastData, data);
            System.exit(-1);
        }
    }

    private void handleUc5(SensorData data) {
        if (data.temperature() > max[data.sensorId() - 1]) {
            max[data.sensorId() - 1] = data.temperature();
        }

        if (data.temperature() < min[data.sensorId() - 1]) {
            min[data.sensorId() - 1] = data.temperature();
        }
    }

    private void handleUc6(SensorData data) {
        totalSum[data.sensorId() - 1] += data.temperature();
    }
}
