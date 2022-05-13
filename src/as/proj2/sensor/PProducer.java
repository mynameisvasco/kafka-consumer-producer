package as.proj2.sensor;

import as.proj2.sensor.Entities.TProducer;
import as.proj2.sensor.Enums.UseCases;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;

public class PProducer {
    public static void main(String[] args) {
        var useCase = args.length == 1 ? UseCases.values()[Integer.parseInt(args[0])] : UseCases.Uc1;
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
                var thread = new TProducer(id, useCase, inputStream);
                thread.start();
                id++;
            } catch (IOException exception) {
                System.out.println("Error! Failed to accept incoming connection");
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


