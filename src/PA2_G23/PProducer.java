package PA2_G23;

import PA2_G23.Entities.TProducer;
import PA2_G23.Enums.UseCases;
import PA2_G23.Models.ConsumerTableModel;
import PA2_G23.Models.SensorData;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class PProducer extends JFrame {
    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedList<ConsumerTableModel> tables = new LinkedList<>();
    private final LinkedList<JLabel> sensorsLabels = new LinkedList<>();
    private final JLabel totalRecordsLabel = new JLabel("Total records: 0");

    public PProducer(UseCases useCase) {
        super("Producers");
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JPanel mainPanel = new JPanel();
        this.setContentPane(mainPanel);
        this.setMinimumSize(new Dimension(1280, 720));
        mainPanel.setLayout(new GridLayout(1, useCase.getKafkaProducersCount() + 2));

        for (int i = 0; i < useCase.getKafkaProducersCount(); i++) {
            var producerPanel = new JPanel();
            producerPanel.setLayout(new BorderLayout(10, 10));
            var table = new JTable();
            var tableModel = new ConsumerTableModel();
            table.setModel(tableModel);
            producerPanel.add(new JLabel(String.format("Producer %1d", i)), BorderLayout.NORTH);
            producerPanel.add(new JScrollPane(table), BorderLayout.CENTER);
            tables.add(tableModel);
            mainPanel.add(producerPanel);
        }

        var totalRecordsPanel = new JPanel();
        totalRecordsPanel.setLayout(new BoxLayout(totalRecordsPanel, BoxLayout.Y_AXIS));

        for (int i = 0; i < 6; i++) {

            var totalRecordsSensorI = new JLabel(String.format("Sensor %d total records: %d", i + 1, 0));
            totalRecordsPanel.add(totalRecordsSensorI, BorderLayout.NORTH);
            sensorsLabels.add(totalRecordsSensorI);
        }

        totalRecordsPanel.add(totalRecordsLabel);
        mainPanel.add(totalRecordsPanel);

        this.pack();
        this.setVisible(true);
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

    public void setTotalRecords(int sensorId, int value) {
        lock.lock();

        try {
            SwingUtilities.invokeAndWait(() -> {
                sensorsLabels.get(sensorId).setText(String.format("Sensor %d total records: %d", sensorId, value));
                long current = Long.parseLong(totalRecordsLabel.getText().split(":")[1].trim());
                totalRecordsLabel.setText(String.format("Total records: %d", current + value));
            });
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        var useCase = args.length == 1 ? UseCases.values()[Integer.parseInt(args[0])] : UseCases.Uc3;
        var pConsumer = new PProducer(useCase);
        ServerSocket server = null;

        try {
            server = new ServerSocket(7192);
            System.out.println("PProducer listening on port 7192");
        } catch (IOException e) {
            System.out.println("Error! Failed to start server on port 7192");
            System.exit(-1);
        }


        int id = 0;

        while (true) {

            try {
                var client = server.accept();
                var inputStream = new ObjectInputStream(client.getInputStream());
                var thread = new TProducer(id, useCase, inputStream, pConsumer);
                thread.start();
                id++;
            } catch (IOException exception) {
                System.out.println("Error! Failed to accept incoming connection");
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


