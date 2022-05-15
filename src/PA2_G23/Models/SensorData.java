package PA2_G23.Models;

import java.io.Serializable;

public record SensorData(int sensorId, Double temperature, int timestamp) implements Serializable {
}