package as.proj2.sensor.Models;

import javax.swing.table.DefaultTableModel;

public class ConsumerTableModel extends DefaultTableModel {
    public ConsumerTableModel() {
        this.addColumn("Sensor Id");
        this.addColumn("Temperature");
        this.addColumn("Timestamp");
    }
}
