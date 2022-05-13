package as.proj2.sensor;

import as.proj2.sensor.Entities.TSource;
import as.proj2.sensor.Enums.UseCases;
import as.proj2.sensor.Models.SensorData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;

public class PSource {
    public static void main(String[] args) {
        var useCase = args.length == 1 ? UseCases.values()[Integer.parseInt(args[0])] : UseCases.Uc1;
        var threads = new LinkedList<TSource>();

        for (int i = 0; i < useCase.getKafkaProducersCount(); i++) {
            var thread = new TSource(i);
            thread.start();
            threads.add(thread);
        }

        try {
            var currentFolder = Paths.get(".");
            var lines = Files.readAllLines(currentFolder.resolve("sensor.txt"));

            for (int i = 0; i < lines.size(); i += 3) {
                var sensorId = Integer.parseInt(lines.get(i));
                var temperature = Double.parseDouble(lines.get(i + 1));
                var timestamp = Integer.parseInt(lines.get(i + 2));
                var threadIndex = (sensorId - 1) % useCase.getKafkaProducersCount();
                threads.get(threadIndex).add(new SensorData(sensorId, temperature, timestamp));
            }

            for (int i = 0; i < useCase.getKafkaProducersCount(); i++) {
                threads.get(i).add(new SensorData(i, 0d,0));
            }

            for (int i = 0; i < useCase.getKafkaProducersCount(); i++) {
                threads.get(i).join();
            }


        } catch (IOException exception) {
            System.out.println("Couldn't read the sensors file");
            exception.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
