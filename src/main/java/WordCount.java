import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.io.*;
import java.util.concurrent.locks.ReentrantLock;
class Pending
{
    // static variable single_instance of type Singleton
    private static Pending single_instance = null;

    // variable of type String
    public Queue<String> queue;

    // private constructor restricted to this class itself
    private Pending()
    {
        queue = new LinkedList<>();
    }

    // static method to create instance of Singleton class
    public static Pending getInstance()
    {
        if (single_instance == null)
            single_instance = new Pending();

        return single_instance;
    }
}
class ActiveWorkers
{
    // static variable single_instance of type Singleton
    private static ActiveWorkers single_instance = null;

    // variable of type String
    public HashMap<Integer, Process> map;

    // private constructor restricted to this class itself
    private ActiveWorkers()
    {
        map = new HashMap<>();
    }

    // static method to create instance of Singleton class
    public static ActiveWorkers getInstance()
    {
        if (single_instance == null)
            single_instance = new ActiveWorkers();

        return single_instance;
    }
}

public class WordCount implements Master, Runnable{

    int numWorkers;
    HashMap<Integer, String> map;
    //HashSet<String> pending;
    HashSet<String> complete;
    HashSet<String> processing;
    List<String> total;
    boolean[] activeWorkers;
    private static final String outputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    static final String inputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    private static final String JAVA_FILE_LOCATION = "/Users/aayushgupta/IdeaProjects/project-2-group-2/src/main/java/";
    private static final int HEARTBEAT_PORT_START = 50001;
    private static final int IO_PORT_START = 55001;

    List<MasterHeartbeat> masterHeartbeatList;

    int curWorkerID;
    int lastHeartbeatPort;
    int lastIOPort;
    public WordCount(int workerNum, String[] filenames) throws IOException {

        numWorkers = workerNum;
        activeWorkers = new boolean[workerNum];
        Arrays.fill(activeWorkers, false);
        map = new HashMap();
        //pending = new HashSet<>();
        total = new LinkedList<>();
        Pending pending = Pending.getInstance();
        for(String s : filenames)
        {
            total.add(s);
            pending.queue.add(s);
        }
        complete = new HashSet<>();
        processing = new HashSet();
        curWorkerID = 0;
        lastHeartbeatPort = HEARTBEAT_PORT_START;
        lastIOPort = IO_PORT_START;
    }

    public void setOutputStream(PrintStream out) {

    }

    public static void main(String[] args) throws IOException{
        WordCount w = new WordCount(2, new String[]{inputDir+"king-james-version-bible.txt"
                ,inputDir+"war-and-peace.txt", inputDir+"test-2.txt"});
        try
        {

            w.compileWorker();
            int n = w.numWorkers;
            while(n > 0)
            {
                w.createWorker();
                n--;
            }
        }
        catch(Exception e)
        {
            System.out.println("Exception in main()");
        }

    }

    public void run() {




    }

public Collection<Process> getActiveProcess() {

        ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
        return new LinkedList<>(activeWorkers.map.values());
    }

    public int compileWorker()
    {
        try
        {
            String command[] = {"javac", "Worker.java"};
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.directory(new File(JAVA_FILE_LOCATION));
            Process process = processBuilder.start();
            process.waitFor();
            return process.exitValue();

        }
        catch(IOException e)
        {
            System.out.println(e.getStackTrace());
            return -1;
        }
        catch (InterruptedException e)
        {
            System.out.println(e.getStackTrace());
            return -1;
        }

    }
    public void createWorker(){

        try
        {
            //Starting Master Heartbeat
            lastHeartbeatPort = HEARTBEAT_PORT_START;
            lastIOPort = IO_PORT_START;
            while(!Utilities.checkPortAvailability(lastHeartbeatPort))
            {
                lastHeartbeatPort++;
            }
            while(!Utilities.checkPortAvailability(lastIOPort))
            {
                lastIOPort++;
            }

            MasterHeartbeat hb = new MasterHeartbeat(curWorkerID, lastHeartbeatPort);
            Thread heartBeatThread = new Thread(hb);
            heartBeatThread.start();

            String[] command = new String[]{
                    "java",
                    "Worker",
                    Integer.toString(curWorkerID),
                    Integer.toString(lastHeartbeatPort),
                    Integer.toString(lastIOPort)};
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.directory(new File(JAVA_FILE_LOCATION)).inheritIO();
            Process process = processBuilder.start();

            MasterIO mio =  new MasterIO(curWorkerID, lastIOPort);
            Thread masterIOThread = new Thread(mio);
            masterIOThread.start();

            //this.activeWorkers[curWorkerID] = true;
            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
            activeWorkers.map.put(curWorkerID, process);
            curWorkerID++;

            //process.waitFor();
            //Process p = Runtime.getRuntime().exec("java Worker 0 50001")
            /*
            Thread.sleep(10000);
            process.destroy();
            if(process.isAlive())
                process.destroyForcibly();
             */
            /*
            if(this.test(55001, 0, inputDir+"king-james-version-bible.txt", outputDir+"out.txt"))
            {
                process.destroyForcibly();
            }

             */

        }
        catch (IOException e)
        {
            System.out.println(e.getStackTrace());
        }
    }
    public boolean test(int socket, int workerID, String inputFilepath, String outFilepath) throws IOException
    {
        ServerSocket sc = null;
        Socket scc = null;
        BufferedReader bfr = null;
        DataOutputStream outputStream = null;
        try {
            sc = new ServerSocket(socket);
            scc = sc.accept();
            outputStream = new DataOutputStream(scc.getOutputStream());
            String filepath = inputFilepath + " " + outFilepath;
            outputStream.writeBytes(filepath + "\n");
            bfr = new BufferedReader(new InputStreamReader(scc.getInputStream()));
            String data = null;
            data = bfr.readLine();
            if (data == null) {
                //handle dead condition
                System.out.println("Worker:" + workerID + " dead");
                return false;
            } else if (data.equals("-1")) {
                System.out.println("Worker failed");
                return false;
            } else {
                System.out.println("Worker successful");
                return true;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
        finally {
            sc.close();
            scc.close();
            bfr.close();
        }
    }
}
class MasterIO implements Runnable
{
    int socketValue;
    int workerID;
    ReentrantLock lock;
    volatile boolean running;
    ServerSocket serverSocket ;
    Socket socket ;
    BufferedReader bufferedReader ;
    DataOutputStream outputStream ;
    private final String inputdir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    private static final String outputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    public MasterIO(int _workerID, int _socket)
    {
        socketValue = _socket;
        workerID = _workerID;
        running = true;
        lock = new ReentrantLock();
        serverSocket = null;
        socket = null;
        bufferedReader = null;
        outputStream = null;
    }
    public void stopThread()
    {
        running = false;
    }
    public void sendIO() throws IOException
    {
        try {
            serverSocket = new ServerSocket(socketValue);
            socket = serverSocket.accept();
            outputStream = new DataOutputStream(socket.getOutputStream());
            while (running) {
                String inputFilepath = null;
                if (lock.tryLock()) {
                    try
                    {
                        Pending pending = Pending.getInstance();
                        ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                        if (pending.queue.isEmpty() || !activeWorkers.map.containsKey(this.workerID)) {
                            System.out.println("if,"+workerID);
                        } else {
                            System.out.println("else,"+workerID);
                            inputFilepath = pending.queue.poll();
                        }
                    }
                    finally {
                        lock.unlock();
                    }

                } else {
                    Thread.sleep(1000);
                }
                if(inputFilepath == null)
                {
                    break;
                }
                String outputFilepath = inputFilepath.substring(inputdir.length());
                String filepath = inputFilepath + " " + outputDir+"out_"+outputFilepath;
                outputStream.writeBytes(filepath + "\n");
                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String data = null;
                data = bufferedReader.readLine();
                if (data == null) {
                    //handle dead condition
                    System.out.println("Worker:" + workerID + " dead");
                    break;
                } else if (data.equals("-1")) {

                    System.out.println("Worker failed");
                } else {
                    System.out.println("Worker successful");
                    continue;
                }
            }
        }
        catch (IOException e)
        {
            System.out.println("sendIO - 2");
        }
        catch(InterruptedException e)
        {
            System.out.println("sendIO - 3");
        }
        finally {
            if(outputStream!= null)
            {
                outputStream.close();
            }
            if(socket != null)
            {
                socket.close();
            }
            if(serverSocket != null)
            {
                serverSocket.close();
            }
        }
    }
    public void run()
    {
        try
        {
            sendIO();
        }
        catch(Exception e)
        {
            System.out.println("Main IO - run() "+e.getStackTrace());
        }
    }


}

class MasterHeartbeat implements Runnable
{
    int socket;
    int workerID;
    volatile boolean running;
    ReentrantLock lock;
    public MasterHeartbeat(int _workerID, int _socket)
    {
        socket = _socket;
        workerID = _workerID;
        running = true;
        lock = new ReentrantLock();
    }
    public void stopThread()
    {
        running = false;
    }
    public void listen() throws IOException
    {
        ServerSocket sc = null;
        Socket scc = null;
        BufferedReader bfr = null;
        try {
            sc = new ServerSocket(socket);
            scc = sc.accept();
            String line = "";
            while(running){
                Thread.sleep(3000);
                bfr = new BufferedReader(new InputStreamReader(scc.getInputStream()));
                String data = null;
                data = bfr.readLine();
                if(data == null)
                {
                    //handle dead condition
                    System.out.println("Worker:"+ workerID + " dead");
                    if(lock.tryLock())
                    {
                        try
                        {
                            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                            activeWorkers.map.remove(workerID);
                            break;
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                    else
                    {
                        Thread.sleep(1000);
                    }
                    //remove from active wrokers
                    //stop master heartbeat
                    //end
                }
                else {
                    //ok
                    System.out.println("Worker: " + data);
                }
                //System.out.println("Inside main heartbeat");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch(InterruptedException e)
        {
            System.out.println("Master Heartbeat for worker" + workerID + "interrupted");
        }
        finally {
            sc.close();
            scc.close();
            bfr.close();
        }
    }
    public void run()
    {
        try
        {
            listen();
        }
        catch(IOException e)
        {
            System.out.println(e.getStackTrace());
        }

    }

}

