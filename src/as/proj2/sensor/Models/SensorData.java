package as.proj2.sensor.Models;

import java.io.Serializable;

public record SensorData(int sensorId, Double temperature, int timestamp) implements Serializable {
}