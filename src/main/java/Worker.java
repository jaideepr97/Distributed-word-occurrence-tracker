import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class Worker{

    int workerId;
    int heartbeatSocketValue;
    int mainSocketValue;
    Socket workerSocket;
    DataOutputStream workerOutputStream;
    BufferedReader bufferedReader;
    public Worker(int _workerId, int _heartbeatSocket, int _mainSocket)
    {
        workerId = _workerId;
        heartbeatSocketValue = _heartbeatSocket;
        mainSocketValue = _mainSocket;
        workerSocket = null;
        workerOutputStream = null;
        bufferedReader = null;
    }
    public void getSocket() throws IOException
    {
        workerSocket = new Socket("localhost", this.mainSocketValue);
        workerOutputStream = new DataOutputStream(workerSocket.getOutputStream());
        bufferedReader = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()));
    }
    public void closeSocket() throws IOException
    {
        if(workerSocket != null) {
            workerSocket.close();
        }
        if(workerOutputStream != null) {
            workerOutputStream.close();
        }
        if(bufferedReader != null) {
            bufferedReader.close();
        }
    }
    public int wordCount(String inputFilename, String outputFilename) throws IOException
    {
        FileReader file = null;
        BufferedReader reader = null;
        HashMap<String, Integer> map = new HashMap();
        DataOutputStream outStream = null;
        FileOutputStream fos = null;
        BufferedWriter writer =  null;
        PriorityQueue<String[]> pq = new PriorityQueue<>(1, new Utilities.CustomComparator());
        try
        {
            file = new FileReader(inputFilename);
            reader = new BufferedReader(file);
            String line = null;
            while((line  = reader.readLine()) != null)
            {
                String[] words = line.split(" ");
                for(String w : words)
                    if(w.length() > 0)
                        map.put(w, map.getOrDefault(w, 0)+1);
            }
            for(Map.Entry<String, Integer> entry : map.entrySet())
            {
                pq.offer(new String[]{entry.getKey(), Integer.toString(entry.getValue())});
            }
            writer = new BufferedWriter(new FileWriter(outputFilename));
            while(!pq.isEmpty())
            {
                String[] val = pq.poll();
                StringBuilder sb = new StringBuilder();
                sb.append(val[0]);
                sb.append(" : ");
                sb.append(val[1]);
                sb.append("\n");
                writer.write(sb.toString());
            }
            return 0;
        }
        catch (FileNotFoundException e)
        {
            System.out.println("Worker(IO): Exception! File not found!!\n");
            return -1;
        }
        catch(IOException e)
        {
            System.out.println("Worker(IO): "+e.getStackTrace()+"\n");
            return -2;
        }
        finally {
            if(reader != null)
            {
                reader.close();
            }
            if(writer != null)
            {
                writer.close();
            }
            if(file != null)
            {
                file.close();
            }
            if(outStream != null)
            {
                outStream.close();
            }
            if(fos != null)
            {
                fos.close();
            }
        }
    }
    public void startWordCount()
    {
        try {
            this.getSocket();
            while(true) {
                String filepath = null;
                filepath = bufferedReader.readLine();
                if(filepath != null)
                {
                    if(filepath.equals("-1"))
                    {
                        System.out.println("Worker(IO): Exiting Worker: " + workerId +"\n");
                        return;
                    }
                    String[] paths = filepath.split(" ");
                    String inputFilepath = paths[0];
                    String outputFilepath = paths[1];
                    System.out.println(workerId+inputFilepath);
                    System.out.println(workerId+outputFilepath);
                    int result = this.wordCount(inputFilepath, outputFilepath);
                    if(result == 0)
                    {
                        this.workerOutputStream.writeBytes(this.workerId+"\n");
                    }
                    else if(result == -1)
                    {
                        this.workerOutputStream.writeBytes(-1+"\n");
                    }
                    else
                    {
                        this.workerOutputStream.writeBytes(-2+"\n");
                    }
                }
            }
        }
        catch (IOException e)
        {
            System.out.println("Worker(IO)(startWordCount): IO Exception: "+workerId+"\n");
        }
        finally {
            try
            {
                System.out.println("Worker(IO)(startWordCount): Closing Socket: "+ workerId + "\n");
                this.closeSocket();
            }
            catch(IOException e)
            {
                System.out.println(e.getStackTrace());
            }
        }
    }
    public static void main(String[] args){

        if(args.length < 3) {
            System.out.println("Worker: Insufficient Arguments\n");
            return;
        }
        Worker w = new Worker(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        WorkerHeartbeat hb = new WorkerHeartbeat(w.workerId, w.heartbeatSocketValue);
        Thread heartBeatThread = new Thread(hb);
        heartBeatThread.start();
        w.startWordCount();
        hb.stopThread();
    }

}

class WorkerHeartbeat implements Runnable{

    private int workerId;
    private int socket;
    Socket workerHeartbeatSocket;
    DataOutputStream workerHeartbeatOutputStream;
    private volatile boolean running;
    public void stopThread()
    {
        running = false;
    }
    public WorkerHeartbeat(int _workerId, int _socket)
    {
        workerId = _workerId;
        socket = _socket;
        workerHeartbeatSocket = null;
        workerHeartbeatOutputStream = null;
        running = true;
    }
    public void getSocket() throws IOException
    {
        workerHeartbeatSocket = new Socket("localhost", socket);
        workerHeartbeatOutputStream = new DataOutputStream(workerHeartbeatSocket.getOutputStream());
    }
    public void closeSocket() throws IOException
    {
        if(workerHeartbeatSocket != null) {
            workerHeartbeatSocket.close();
        }
        if(workerHeartbeatOutputStream != null) {
            workerHeartbeatOutputStream.close();
        }
    }
    public void sendMessage() throws IOException
    {

        try{
            workerHeartbeatOutputStream.writeBytes(workerId+"\n");
        }
        catch (IOException e){
           System.out.println("Worker(WorkerHeartbeat)(sendMessage): IOException"+workerId+"\n");
        }
    }
    public void run()  {
        System.out.println("Worker(WorkerHeartbeat): Running Worker Heartbeat for: " +  workerId + "\n");
        try {
            getSocket();
            while(running) {
                    sendMessage();
                    System.out.println("Worker(WorkerHeartbeat): " + workerId + ", " + "I'm alive\n");
                    // Let the thread sleep for a while.
                    Thread.sleep(3 * 1000);
            }
        }
        catch (IOException e)
        {
            System.out.println("Worker(WorkerHeartbeat)(run): IOException: "+workerId+"\n");
        }
        catch(InterruptedException e)
        {
            System.out.print("Worker(WorkerHeartbeat): Worker: " +workerId+ "interrupted\n");
        }
        finally {
            try
            {
                System.out.println("Worker(WorkerHeartbeat): Closing Socket: "+workerId+"\n");
                closeSocket();
            }
            catch(IOException e)
            {
                System.out.println("Worker(WorkerHeartbeat)(run): IOException: "+workerId+"\n");
            }
        }
        System.out.println("Worker(WorkerHeartbeat): Worker Heartbeat Thread: " +  workerId + " exiting.\n");
    }
}