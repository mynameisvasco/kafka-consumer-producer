package as.proj2.sensor.Entities;

import as.proj2.sensor.Models.SensorData;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TSource extends Thread {
    private final int id;
    private final Socket client;
    private final LinkedList<SensorData> queue;
    private final ReentrantLock lock;
    private final Condition isNotEmptyCond;

    public TSource(int id) {
        this.id = id;
        queue = new LinkedList<>();
        lock = new ReentrantLock();
        isNotEmptyCond = lock.newCondition();
        client = new Socket();

        try {
            client.connect(new InetSocketAddress(7192));
        } catch (IOException exception) {
            System.out.println("Couldn't connect to server, retrying... Reason: " + exception.getMessage());
        }
    }

    public void add(SensorData data) {
        try {
            lock.lock();
            queue.add(data);
            isNotEmptyCond.signal();
        } finally {
            lock.unlock();
        }
    }

    private SensorData get() {
        try {
            lock.lock();

            while (queue.isEmpty()) {
                try {
                    isNotEmptyCond.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return queue.remove();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        try {
            var outputStream = new ObjectOutputStream(client.getOutputStream());

            while (true) {
                var data = get();
                outputStream.writeObject(data);
                outputStream.flush();

                if (data.temperature() == 0 && data.timestamp() == 0) {
                    break;
                }

                System.out.format("TSource %d: %s\n", id, data);
            }
        } catch (IOException e) {
            System.out.println("Error! Not possible to write to socket");
        }

    }
}
