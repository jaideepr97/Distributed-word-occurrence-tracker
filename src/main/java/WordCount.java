import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

//Singleton queue to store the pending files
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
    public static synchronized Pending getInstance()
    {
        if (single_instance == null)
            single_instance = new Pending();

        return single_instance;
    }
}
//Singleton set to store the active workers
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
    public static synchronized ActiveWorkers getInstance()
    {
        if (single_instance == null)
            single_instance = new ActiveWorkers();

        return single_instance;
    }
}
//Singleton map to store the files being processed
class Processing
{
    // static variable single_instance of type Singleton
    private static Processing single_instance = null;

    // variable of type String
    public HashMap<Integer, String> map;

    // private constructor restricted to this class itself
    private Processing()
    {
        map = new HashMap<>();
    }

    // static method to create instance of Singleton class
    public static synchronized Processing getInstance()
    {
        if (single_instance == null)
            single_instance = new Processing();

        return single_instance;
    }
}
//Singleton set to store the inactive workers
class InactiveWorkers
{
    // static variable single_instance of type Singleton
    private static InactiveWorkers single_instance = null;

    // variable of type String
    public HashSet<Integer> set;

    // private constructor restricted to this class itself
    private InactiveWorkers()
    {
        set = new HashSet<>();
    }

    // static method to create instance of Singleton class
    public static synchronized InactiveWorkers getInstance()
    {
        if (single_instance == null)
            single_instance = new InactiveWorkers();

        return single_instance;
    }
}
public class WordCount implements Master, Runnable{

    int numWorkers;
    HashMap<Integer, String> map;
    HashSet<String> complete;
    HashSet<String> processing;
    List<String> total;
    private static final String outputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/out/";
    private static final String finalOutputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/finalout/";
    private static final String JAVA_FILE_LOCATION = "/Users/aayushgupta/IdeaProjects/project-2-group-2/src/main/java/";
    private static final int HEARTBEAT_PORT_START = 50001;
    private static final int IO_PORT_START = 55001;
    static ReentrantLock lock;
    private PrintStream outputstream;
    int curWorkerID;
    int lastHeartbeatPort;
    int lastIOPort;


    public WordCount(int workerNum, String[] filenames){

        numWorkers = workerNum;
        map = new HashMap();
        total = new LinkedList<>();
        Pending pending = Pending.getInstance();
        for(String s : filenames)
        {
            total.add(s);
            pending.queue.add(s);
        }
        InactiveWorkers inactiveWorkers = InactiveWorkers.getInstance();
        for(int i=0; i<numWorkers;i++)
        {
            inactiveWorkers.set.add(i);
        }
        complete = new HashSet<>();
        processing = new HashSet();
        curWorkerID = 0;
        lastHeartbeatPort = HEARTBEAT_PORT_START;
        lastIOPort = IO_PORT_START;
        lock = new ReentrantLock();
        outputstream = null;
    }

    public void setOutputStream(PrintStream out) {

        this.outputstream = out;
    }

    public static void main(String[] args) throws IOException{

        String path = "/Users/aayushgupta/IdeaProjects/project-2-group-2/";
        InputStreamReader reader = new InputStreamReader(new FileInputStream(path+"fileNames.txt"), "UTF-8");
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter("\n");
        List<String> paths = new ArrayList<>();
        while(scanner.hasNext())
        {
            String s = scanner.next();
            paths.add(s);
        }
        String[] pathArr = new String[paths.size()];
        for(int i=0; i<pathArr.length; i++) pathArr[i] = paths.get(i);
        WordCount w = new WordCount(15, pathArr);
        w.run();
    }


    public void run() {
        try {
            this.compileWorker();
            HashSet<Integer> inactive = new HashSet(InactiveWorkers.getInstance().set);
            for (int id : inactive) {
                this.curWorkerID = id;
                this.createWorker();
            }
            int i = 0;
            boolean flag = false;
            while (true) {
                Thread.sleep(1000);
                System.out.println("Try:" + i++);
                if (lock.tryLock()) {
                    try {
                        Processing processing = Processing.getInstance();
                        Pending pending = Pending.getInstance();
                        ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                        inactive = new HashSet<Integer>(InactiveWorkers.getInstance().set);
                        System.out.println("Master(Main)(run): Processing Size:" + processing.map.size() + " " + "Pending Size:" + pending.queue.size() + "\n");
                        synchronized (processing.map) {
                            if (pending.queue.isEmpty() && processing.map.isEmpty()) {
                                System.out.println("Master(Main)(run): Starting merge\n");
                                flag = true;
                                break;
                            } else if (inactive.size() > 0 && !pending.queue.isEmpty()) {
                                System.out.println("Master(Main)(run): Inactive Workers found: " + inactive.size() + "\n");
                                for (int id : inactive) {
                                    System.out.println("Master(Main)(run): Creating worker for: " + id + "\n");
                                    this.curWorkerID = id;
                                    this.createWorker();
                                }
                            } else {
                                System.out.println("Master(Main)(run): Still processing\n");
                            }
                        }
                    } finally {
                        lock.unlock();
                        System.out.println("Master: Lock Released");

                    }
                }
            }
            if (flag) {
                Collection<Process> activeWorkers = getActiveProcess();
                for(Process a : activeWorkers)
                {
                    a.destroyForcibly();
                }
                if (merge()) {
                    System.out.println("Master(Main)(run): Merge successful\n");
                }
            }

        }
        catch(InterruptedException e)
        {
            System.out.println("Master(Main)(run): Interrupted Exception");
        }
        catch (IOException e)
        {
            System.out.println("Master(Main)(run): IO Exception");
        }
    }
public boolean merge() throws IOException
{
    Reader reader = null;
    Scanner scanner = null;
    HashMap<String, Integer> map = new HashMap();
    DataOutputStream outStream = null;
    FileOutputStream fos = null;
    PriorityQueue<String[]> pq = new PriorityQueue<>(1, new Utilities.CustomComparator());
    FileReader fileReader = null;
    BufferedReader bufferedReader =  null;
    File file = null;
    File[] paths;
    try
    {
        file = new File(outputDir);
        paths = file.listFiles();
        for(File f : paths)
        {
            if(f.isHidden()) continue;
            int i=0;
            fileReader = new FileReader(f);
            bufferedReader = new BufferedReader(fileReader);
            String line = null;
            while((line  = bufferedReader.readLine()) != null)
            {
                String[] values = line.trim().split(" : ");
                String word = values[0].trim();
                String count = values[1].trim();
                map.put(word, map.getOrDefault(word, 0)+Integer.parseInt(count));
            }
        }
        System.out.println("Master(Main)(merge): Done mapping\n");
        for(Map.Entry<String, Integer> entry : map.entrySet())
        {
            pq.offer(new String[]{entry.getKey(), Integer.toString(entry.getValue())});
        }
        //change filename
        fos = new FileOutputStream(finalOutputDir+"finalout.txt");
        outStream = new DataOutputStream(new BufferedOutputStream(fos));
        while(!pq.isEmpty())
        {
            String[] val = pq.poll();
            StringBuilder sb = new StringBuilder();
            sb.append(val[1]);
            sb.append(" : ");
            sb.append(val[0]);
            if(!pq.isEmpty())
            {
                sb.append("\n");
            }
            this.outputstream.print(sb.toString());
            //Uncomment this to write to file
            // outStream.writeBytes(sb.toString());
        }
        this.outputstream.print("\n");
        for(File f : paths)
        {
            f.delete();
        }
        return true;
    }
    catch (FileNotFoundException e)
    {
        System.out.println("Master(Main)(run): File not found!!\n");
        return false;
    }
    catch(IOException e)
    {
        System.out.println(e.getStackTrace());
        return false;
    }
    catch (Exception e)
    {
        System.out.println(e.toString());
        return false;
    }
    finally {
        if(fileReader != null)
        {
            fileReader.close();
        }
        if(bufferedReader != null)
        {
            bufferedReader.close();
        }
        if(reader != null)
        {
            reader.close();
        }
        if(scanner != null)
        {
            scanner.close();
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
public Collection<Process> getActiveProcess() {

        int counter = 3;
        while(counter > 0)
        {
            if(lock.tryLock())
            {
                try
                {
                    ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                    return new LinkedList<>(activeWorkers.map.values());
                }
                catch (Exception e)
                {
                    return null;
                }
                finally
                {
                    lock.unlock();
                }
            }
            else
            {
                try
                {
                    counter--;
                    Thread.sleep(1000);
                }
                catch(InterruptedException e)
                {
                    System.out.println(e.getStackTrace());
                }
            }
        }
        return null;
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
    public void createWorker() {
        ReentrantLock lock = new ReentrantLock();
        if (lock.tryLock()) {
            try {
                //Starting Master Heartbeat
                lastHeartbeatPort = HEARTBEAT_PORT_START;
                lastIOPort = IO_PORT_START;
                while (!Utilities.checkPortAvailability(lastHeartbeatPort)) {
                    lastHeartbeatPort++;
                }
                while (!Utilities.checkPortAvailability(lastIOPort)) {
                    lastIOPort++;
                }

                String[] command = new String[]{
                        "java",
                        "Worker",
                        Integer.toString(curWorkerID),
                        Integer.toString(lastHeartbeatPort),
                        Integer.toString(lastIOPort)};
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.directory(new File(JAVA_FILE_LOCATION)).inheritIO();
                MasterHeartbeat hb = new MasterHeartbeat(curWorkerID, lastHeartbeatPort);
                Thread heartBeatThread = new Thread(hb);
                heartBeatThread.start();
                Process process = processBuilder.start();

                ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                activeWorkers.map.put(curWorkerID, process);
                InactiveWorkers inactiveWorkers = InactiveWorkers.getInstance();
                inactiveWorkers.set.remove(curWorkerID);
                MasterIO mio = new MasterIO(curWorkerID, lastIOPort);
                Thread masterIOThread = new Thread(mio);
                masterIOThread.start();
                System.out.println("Master(Main)(createWorker): Created Worker: " +  curWorkerID+"\n");
            } catch (IOException e) {
                System.out.println(e.getStackTrace());
            }

        }
    }
}
class MasterIO implements Runnable
{
    int socketValue;
    int workerID;
    static ReentrantLock lock;
    volatile boolean running;
    ServerSocket serverSocket ;
    Socket socket ;
    BufferedReader bufferedReader ;
    DataOutputStream outputStream ;
    private final String inputdir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    private static final String outputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/out/";
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
    public void sendIO() throws IOException, InterruptedException
    {
        try {
            serverSocket = new ServerSocket(socketValue);
            socket = serverSocket.accept();
            outputStream = new DataOutputStream(socket.getOutputStream());
            while (running) {
                int counter = 3;
                String inputFilepath = null;
                while(counter > 0)
                {
                    if (lock.tryLock()) {
                        try
                        {
                            Pending pending = Pending.getInstance();
                            Processing processing = Processing.getInstance();
                            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                            synchronized (processing.map) {
                                if (pending.queue.isEmpty() || !activeWorkers.map.containsKey(this.workerID)) {
                                    System.out.println("Master(MasterIO)(sendIO): Pending Empty or Inactive Worker: " + workerID + "\n");
                                    break;
                                } else {
                                    System.out.println("Master(MasterIO)(sendIO): Pending contains data and Active Worker: " + workerID + "\n");
                                    inputFilepath = pending.queue.poll();
                                    if(inputFilepath == null) break;
                                    processing.map.put(workerID, inputFilepath);
                                }
                                break;
                            }
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                    else {
                        counter--;
                        Thread.sleep(1000);
                    }
                }
                if(inputFilepath == null)
                {
                    outputStream.writeBytes("-1" + "\n");
                    break;
                }

                String outputFilepath = inputFilepath.replaceAll("[^a-zA-Z0-9]", "")+".txt";
                String filepath = inputFilepath + " " + outputDir+outputFilepath;
                outputStream.writeBytes(filepath + "\n");
                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String data = null;
                data = bufferedReader.readLine();
                if (data == null || data.equals("-1") || data.equals("-2")) {
                    counter = 3;
                    while(counter > 0)
                    {
                        if(lock.tryLock())
                        {
                            try
                            {
                                System.out.print("Master(MasterIO)(sendIO): Worker Dead, removing from processing: " + workerID+"\n");
                                Processing processing = Processing.getInstance();
                                Pending pending = Pending.getInstance();
                                synchronized (processing.map) {
                                    String filePath = processing.map.get(workerID);
                                    processing.map.remove(workerID);
                                    if(data != null && !data.equals("-1")) {
                                        pending.queue.offer(filePath);
                                    }
                                    break;
                                }
                            }
                            finally {
                                lock.unlock();
                            }
                        }
                        else
                        {
                            counter--;
                            Thread.sleep(1000);
                        }
                    }
                    System.out.println("Master(MasterIO)(sendIO): Worker:" + workerID + " dead\n");
                }
                else {
                    while(true)
                    {
                        boolean tryLock = lock.tryLock(1, TimeUnit.SECONDS);
                        if(tryLock)
                        {
                            try
                            {
                                Processing processing = Processing.getInstance();
                                synchronized (processing.map) {
                                    processing.map.remove(workerID);
                                    break;
                                }
                            }
                            finally {
                                lock.unlock();
                            }
                        }
                        else continue;
                    }
                    System.out.println("Master(MasterIO)(sendIO): Worker successful: "+workerID+"\n");
                    //Thread.sleep(2000);
                    continue;
                }
            }
        }
        catch(Exception e)
        {
            System.out.println("Master(MasterIO)(sendIO): Exception: "+e.getMessage()+"\n");
            //int counter = 5;
            while(true) {
                boolean tryLock = lock.tryLock(1, TimeUnit.SECONDS);
                if (tryLock) {
                    try {
                        Processing processing = Processing.getInstance();
                        Pending pending = Pending.getInstance();
                        synchronized (processing.map) {
                            String file = processing.map.get(workerID);
                            pending.queue.offer(file);
                            processing.map.remove(workerID);
                            break;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                else continue;
            }
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
            System.out.println("Master(MasterIO)(run): Starting IO for Worker: "+workerID+"\n");
            sendIO();
        }
        catch(Exception e)
        {
            System.out.println("Master(MasterIO)(run): Exception in run(): "+e.getStackTrace());
        }
    }


}

class MasterHeartbeat implements Runnable
{
    int socket;
    int workerID;
    volatile boolean running;
    static ReentrantLock lock;
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
            String lastInputFilepath = "";
            while(running){
                Thread.sleep(3000);
                bfr = new BufferedReader(new InputStreamReader(scc.getInputStream()));
                String data = null;
                data = bfr.readLine();
                if(data == null)
                {
                    System.out.println("Master(MasterHeartbeat)(listen): Worker: "+ workerID + " dead\n");
                    if(lock.tryLock())
                    {
                        try
                        {
                            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                            InactiveWorkers inactiveWorkers = InactiveWorkers.getInstance();
                            activeWorkers.map.remove(workerID);
                            inactiveWorkers.set.add(workerID);
                            break;
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                    else {
                        Thread.sleep(1000);
                    }
                }
                else {
                    System.out.println("Master(MasterHeartbeat)(listen): Worker Alive: " + workerID);
                }
            }
        }
        catch (IOException e)
        {
            System.out.println("Master(MasterHeartbeat)(listen): Master Heartbeat for worker" + workerID + "IO exception\n");
        }
        catch(InterruptedException e)
        {
            System.out.println("Master(MasterHeartbeat)(listen): Master Heartbeat for worker" + workerID + "interrupted\n");
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
            System.out.println("Master(MasterHeartbeat)(run): Starting listen for Worker: "+workerID+"\n");
            listen();
        }
        catch(IOException e)
        {
            System.out.println("Master(MasterHeartbeat)(run): IO Exception\n");
        }

    }

}

