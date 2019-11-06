import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class Worker implements Runnable {

    public void run() {

        while(true)
        {
            try
            {
                System.out.println("Worker Main Thread");
                Thread.sleep(5000);
            }
            catch(InterruptedException e)
            {
                System.out.println(e.getStackTrace());
            }

        }
    }

    public static void main(String[] args) {

        WorkerHeartbeat hb = new WorkerHeartbeat(0, 50001);
        Thread heartBeatThread = new Thread(hb);
        Worker w = new Worker();
        Thread mainThread = new Thread(w);
        heartBeatThread.start();
        mainThread.start();
    }
}

class WorkerHeartbeat implements Runnable{

    private int workerId;
    private int socket;
    public WorkerHeartbeat(int _workerId, int _socket)
    {
        workerId = _workerId;
        socket = _socket;
    }
    public void sendMessage() throws IOException
    {
        Socket sc = null; // Need to initialize as we are closing in finally block
        DataOutputStream dout = null;
        try{
            //Binding socket to 50001 port number on localhost
            sc = new Socket("localhost", socket);
            dout = new DataOutputStream(sc.getOutputStream());
            dout.writeBytes(Integer.toString(workerId) + "\n");
        }
        catch (IOException e){
            e.printStackTrace();
        }
        finally {
            sc.close();
            dout.close();
        }
    }
    public void run() {
        System.out.println("Running Worker Heartbeat:" +  workerId);
        try {
            while(true) {
                System.out.println("Worker Heartbeat Thread: " + workerId + ", " + "I'm alive");
                // Let the thread sleep for a while.
                Thread.sleep(3 * 1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Worker Heartbeat Thread: " +  workerId + " interrupted.");
        }
        System.out.println("Worker Heartbeat Thread: " +  workerId + " exiting.");
    }
}