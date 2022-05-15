package PA2_G23.Models;

import javax.swing.table.DefaultTableModel;

public class ProducerTableModel extends DefaultTableModel {
    public ProducerTableModel() {
        this.addColumn("Sensor Id");
        this.addColumn("Temperature");
        this.addColumn("Timestamp");
    }
}
