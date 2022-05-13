package as.proj2.sensor;

import as.proj2.sensor.Entities.TConsumer;
import as.proj2.sensor.Enums.UseCases;
import as.proj2.sensor.Models.ConsumerTableModel;
import as.proj2.sensor.Models.SensorData;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class PConsumer extends JFrame {
    private JPanel mainPanel = new JPanel();
    private ReentrantLock lock = new ReentrantLock();
    private LinkedList<ConsumerTableModel> tables = new LinkedList<>();
    private LinkedList<JPanel> groupPanels = new LinkedList<>();
    private LinkedList<JLabel> totalRecordsBySensorIdLabels = new LinkedList<>();
    private JLabel totalRecordsLabel = new JLabel("Total records: 0");

    public PConsumer(UseCases useCase) {
        super("Consumers");
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setContentPane(mainPanel);
        this.setMinimumSize(new Dimension(1280, 720));
        mainPanel.setLayout(new GridLayout(1, useCase.getKafkaConsumerGroupsCount() + 2));

        for (int i = 0; i < useCase.getKafkaConsumerGroupsCount(); i++) {
            var groupPanel = new JPanel();
            groupPanel.setLayout(new GridLayout(useCase.getKafkaConsumerGroupsCount(), 1));
            groupPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), String.format("Group %d", i), TitledBorder.CENTER, TitledBorder.TOP));
            this.add(groupPanel);
            groupPanels.add(groupPanel);
        }

        for (int i = 0; i < useCase.getKafkaConsumersCount(); i++) {
            var groupPanel = groupPanels.get(i % useCase.getKafkaConsumerGroupsCount());
            var consumerPanel = new JPanel();
            consumerPanel.setLayout(new BorderLayout(10, 10));
            var table = new JTable();
            var tableModel = new ConsumerTableModel();
            table.setModel(tableModel);
            consumerPanel.add(new JLabel(String.format("Consumer %1d", i)), BorderLayout.NORTH);
            consumerPanel.add(new JScrollPane(table), BorderLayout.CENTER);
            groupPanel.add(consumerPanel);
            tables.add(tableModel);
        }


        var totalRecordsPanel = new JPanel();
        totalRecordsPanel.setLayout(new BoxLayout(totalRecordsPanel, BoxLayout.Y_AXIS));

        for (int i = 0; i < 6; i++) {

            var totalRecordsSensorI = new Label(String.format("Sensor %d total records: %d", i + 1, 0));
            totalRecordsPanel.add(totalRecordsSensorI, BorderLayout.NORTH);
        }

        totalRecordsPanel.add(totalRecordsLabel);
        mainPanel.add(totalRecordsPanel);

        this.pack();
        this.setVisible(true);
    }

    private LinkedList<TConsumer> startThreads(UseCases useCase) {
        var threads = new LinkedList<TConsumer>();


        for (int i = 0; i < useCase.getKafkaConsumersCount(); i++) {
            var thread = new TConsumer(i, useCase, this);
            thread.start();
            threads.add(thread);
        }

        return threads;
    }

    private void joinThreads(UseCases useCase, LinkedList<TConsumer> threads) {
        for (int i = 0; i < useCase.getKafkaConsumersCount(); i++) {
            var thread = threads.get(i);

            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void addRecords(int consumerId, List<SensorData> records) {
        lock.lock();

        try {
            SwingUtilities.invokeAndWait(() -> {
                for (var record : records) {
                    tables.get(consumerId).insertRow(0, new Object[]{record.sensorId(), record.temperature(), record.timestamp()});
                }
            });
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void setTotalRecords(int consumerId, int value) {
        lock.lock();

        try {
            SwingUtilities.invokeAndWait(() -> {
                totalRecordsBySensorIdLabels.get(consumerId).setText(String.format("Sensor %d total records: %d", consumerId, value));
            });
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            lock.unlock();
        }
    }


    public static void main(String[] args) {
        var useCase = args.length == 1 ? UseCases.values()[Integer.parseInt(args[0])] : UseCases.Uc1;
        var pConsumer = new PConsumer(useCase);
        var threads = pConsumer.startThreads(useCase);
        pConsumer.joinThreads(useCase, threads);

        var min = new LinkedList<Double>();
        var max = new LinkedList<Double>();
        var avg = new LinkedList<Double>();

        for (int i = 0; i < useCase.getKafkaConsumersCount(); i++) {
            min.push(threads.get(i).getMinTemperature());
            max.push(threads.get(i).getMaxTemperature());
            avg.push(threads.get(i).getAverage());
        }

        if (useCase == UseCases.Uc5) {
            for (int i = 0; i < useCase.getKafkaConsumersCount(); i++) {
                System.out.printf("Max: %f, Min: %f\n", max.get(i), min.get(i));
            }
        }

        if (useCase == UseCases.Uc6) {
            for (int i = 0; i < useCase.getKafkaConsumersCount(); i++) {
                System.out.printf("Avg: %f\n", avg.get(i));
            }
        }
    }
}
