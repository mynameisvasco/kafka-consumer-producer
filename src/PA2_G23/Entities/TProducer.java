package PA2_G23.Entities;

import PA2_G23.PProducer;
import PA2_G23.Enums.UseCases;
import PA2_G23.Models.SensorData;
import PA2_G23.Util;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.LinkedList;
import java.util.Properties;

public class TProducer extends Thread {
    private final int id;
    private final UseCases useCase;
    private final Producer<String, byte[]> producer;
    private final ObjectInputStream inputStream;
    private final PProducer gui;
    private final int[] totalRecords = {0, 0, 0, 0, 0, 0};

    public TProducer(int id, UseCases useCase, ObjectInputStream inputStream, PProducer gui) {
        this.id = id;
        this.useCase = useCase;
        this.inputStream = inputStream;
        this.gui = gui;
        var properties = new Properties();
        Util.configureProducer(useCase, properties);
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        var dataList = new LinkedList<SensorData>();
        var isExit = false;

        while (true) {
            try {
                var data = (SensorData) inputStream.readObject();
                handleSendUc1(data);

                if (dataList.size() >= 10000 || data.timestamp() == 0) {
                    gui.addRecords(id, new LinkedList<>(dataList));
                    dataList.clear();
                }

                if (data.timestamp() == 0) {
                    isExit = true;
                } else {
                    totalRecords[data.sensorId() - 1]++;
                    dataList.add(data);
                }

                if (isExit) break;
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 6; i++) {
            if (totalRecords[i] != 0) {
                gui.setTotalRecords(i, totalRecords[i]);
            }
        }
    }

    private void handleSendUc1(SensorData data) {
        if (data.timestamp() == 0) {
            var a = 0;
        }

        var key = String.format("%s", (data.sensorId() - 1) % useCase.getKafkaPartitionsCount());
        var record = new ProducerRecord<>("Sensor", key, SerializationUtils.serialize(data));
        producer.send(record);
    }
}