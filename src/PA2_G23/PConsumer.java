package PA2_G23;

import PA2_G23.Entities.TConsumer;
import PA2_G23.Models.SensorData;
import PA2_G23.Enums.UseCases;
import PA2_G23.Models.ConsumerTableModel;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class PConsumer extends JFrame {
    private ReentrantLock lock = new ReentrantLock();
    private final LinkedList<ConsumerTableModel> tables = new LinkedList<>();
    private final LinkedList<JLabel> totalRecordsBySensorIdLabels = new LinkedList<>();
    private final JLabel totalRecordsLabel = new JLabel("Total records: 0");
    private final LinkedList<LinkedList<Double>> min = new LinkedList<>();
    private final LinkedList<LinkedList<Double>> max = new LinkedList<>();

    public PConsumer(UseCases useCase) {
        super("Consumers");
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JPanel mainPanel = new JPanel();
        this.setContentPane(mainPanel);
        this.setMinimumSize(new Dimension(1280, 720));
        mainPanel.setLayout(new GridLayout(1, useCase.getKafkaConsumerGroupsCount() + 2));

        LinkedList<JPanel> groupPanels = new LinkedList<>();
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
            JLabel label = null;

            if (useCase == UseCases.Uc6) {
                label = new JLabel(String.format("Sensor %d total records: 0, Avg Temp: 0", i + 1));
            } else if (useCase == UseCases.Uc5) {
                label = new JLabel(String.format("Sensor %d total records: 0, Min Temp: 0, Max Temp: 0", i + 1));
            } else {
                label = new JLabel(String.format("Sensor %d total records: 0", i + 1));
            }

            totalRecordsPanel.add(label, BorderLayout.NORTH);
            totalRecordsBySensorIdLabels.add(label);
            min.add(new LinkedList<>());
            max.add(new LinkedList<>());
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
                if (tables.get(consumerId).getRowCount() > 100000) {
                    tables.get(consumerId).getDataVector().removeAllElements();
                    tables.get(consumerId).fireTableDataChanged();
                }

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

    public void setTotalRecords(int sensorId, int total) {
        lock.lock();

        try {
            SwingUtilities.invokeAndWait(() -> {
                totalRecordsBySensorIdLabels.get(sensorId).setText(String.format("Sensor %d total records: %d", sensorId, total));
                long current = Long.parseLong(totalRecordsLabel.getText().split(":")[1].trim());
                totalRecordsLabel.setText(String.format("Total records: %d", current + total));
            });
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void setTotalRecords(int sensorId, int total, double avg) {
        lock.lock();

        try {
            SwingUtilities.invokeAndWait(() -> {
                totalRecordsBySensorIdLabels.get(sensorId).setText(String.format("Sensor %d total records: %d, Avg Temp: %f", sensorId, total, avg));
                long current = Long.parseLong(totalRecordsLabel.getText().split(":")[1].trim());
                totalRecordsLabel.setText(String.format("Total records: %d", current + total));
            });
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void setTotalRecords(int sensorId, int total, double min, double max) {
        lock.lock();

        try {
            var votes = this.min.get(sensorId).size() + 1;

            if (votes >= 3) {
                SwingUtilities.invokeAndWait(() -> {
                    var votedMin = Util.mostCommon(this.min.get(sensorId));
                    var votedMax = Util.mostCommon(this.max.get(sensorId));
                    totalRecordsBySensorIdLabels.get(sensorId).setText(String.format("Sensor %d total records: %d, Min Temp: %f, Max Temp: %f", sensorId, total, votedMin, votedMax));
                });
            } else {
                this.min.get(sensorId).add(min);
                this.max.get(sensorId).add(max);
            }

            long current = Long.parseLong(totalRecordsLabel.getText().split(":")[1].trim());
            totalRecordsLabel.setText(String.format("Total records: %d", current + total));

        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        var useCase = args.length == 1 ? UseCases.values()[Integer.parseInt(args[0])] : UseCases.Uc3;
        var pConsumer = new PConsumer(useCase);
        var threads = pConsumer.startThreads(useCase);
        pConsumer.joinThreads(useCase, threads);
    }
}
