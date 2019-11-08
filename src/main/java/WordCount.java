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
class Processing
{
    // static variable single_instance of type Singleton
    private static Processing single_instance = null;

    // variable of type String
    public HashSet<String> set;

    // private constructor restricted to this class itself
    private Processing()
    {
        set = new HashSet<>();
    }

    // static method to create instance of Singleton class
    public static Processing getInstance()
    {
        if (single_instance == null)
            single_instance = new Processing();

        return single_instance;
    }
}
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
    public static InactiveWorkers getInstance()
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
    static final String inputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    private static final String JAVA_FILE_LOCATION = "/Users/aayushgupta/IdeaProjects/project-2-group-2/src/main/java/";
    private static final int HEARTBEAT_PORT_START = 50001;
    private static final int IO_PORT_START = 55001;
    ReentrantLock lock;
    private PrintStream outputstream;
    int curWorkerID;
    int lastHeartbeatPort;
    int lastIOPort;


    public WordCount(int workerNum, String[] filenames) throws IOException {

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

        WordCount w = new WordCount(3, new String[]{inputDir+"king-james-version-bible.txt"
                ,inputDir+"war-and-peace.txt", inputDir+"test-2.txt"});
        w.run();

    }

    public void run() {
        try
        {
            this.compileWorker();
            HashSet<Integer> inactive = new HashSet(InactiveWorkers.getInstance().set);
            for(int id : inactive)
            {
                this.curWorkerID = id;
                this.createWorker();
            }
            int i=0;
            boolean flag = false;
            while(true)
            {
                Thread.sleep(3000);
                System.out.println("Try:" + i++);
                if(lock.tryLock())
                {
                    try
                    {
                        Processing processing = Processing.getInstance();
                        Pending pending = Pending.getInstance();
                        ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                        inactive = new HashSet<Integer>(InactiveWorkers.getInstance().set);
                        if(pending.queue.isEmpty() && processing.set.isEmpty())
                        {
                            System.out.println("Starting merge");
                            flag = true;
                            break;
                        }
                        else if(inactive.size() > 0)
                        {
                            for(int id : inactive)
                            {
                                this.curWorkerID = id;
                                this.createWorker();
                            }
                        }
                        else
                        {
                            System.out.println("Still processing");
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
            if(flag)
            {
                /*
                Collection<Process> activeWorkers = getActiveProcess();
                for(Process a : activeWorkers)
                {
                    a.destroyForcibly();
                }
                */
                if(merge())
                {
                    System.out.println("Merge successful");
                }
            }

        }
        catch(InterruptedException e)
        {
            System.out.println(e.getStackTrace());
        }
        catch (IOException e)
        {
            System.out.println(e.getStackTrace());
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
    File file = null;
    File[] paths;
    try
    {
        //System.out.println(inputFilename);
        file = new File(outputDir);
        paths = file.listFiles();
        for(File f : paths)
        {
            if(f.isHidden()) continue;
            int i=0;
            reader = new FileReader(f);
            scanner = new Scanner(reader);
            scanner.useDelimiter("\n");
            System.out.println(f);
            while(scanner.hasNext())
            {
                String s = scanner.next();
                String[] values = s.split(" ");
                String word = values[0].trim();
                String count = values[2].trim();
                //System.out.println(word + " "+ count);
                map.put(word, map.getOrDefault(word, 0)+Integer.parseInt(count));
            }
        }
        System.out.println("Done mapping");
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
            sb.append(val[0]);
            sb.append(" : ");
            sb.append(val[1]);
            if(!pq.isEmpty())
            {
                sb.append("\n");
            }
            //this.outputstream.println(sb.toString());
            outStream.writeBytes(sb.toString());
        }
        return true;
    }
    catch (FileNotFoundException e)
    {
        System.out.println("File not found!!");
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

            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
            activeWorkers.map.put(curWorkerID, process);

            MasterIO mio =  new MasterIO(curWorkerID, lastIOPort);
            Thread masterIOThread = new Thread(mio);
            masterIOThread.start();

            //this.activeWorkers[curWorkerID] = true;


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
    public void sendIO() throws IOException
    {
        try {
            serverSocket = new ServerSocket(socketValue);
            socket = serverSocket.accept();
            outputStream = new DataOutputStream(socket.getOutputStream());
            while (running) {
                String inputFilepath = null;
                int counter = 3;
                while(counter > 0)
                {
                    if (lock.tryLock()) {
                        try
                        {
                            Pending pending = Pending.getInstance();
                            Processing processing = Processing.getInstance();
                            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
                            if (pending.queue.isEmpty() || !activeWorkers.map.containsKey(this.workerID)) {
                                System.out.println("if,"+workerID);
                            } else {
                                System.out.println("else,"+workerID);
                                inputFilepath = pending.queue.poll();
                                processing.set.add(inputFilepath);
                            }
                            break;
                        }
                        finally {
                            lock.unlock();
                        }

                    } else {
                        counter--;
                        Thread.sleep(1000);
                    }
                }
                if(inputFilepath == null)
                {
                    outputStream.writeBytes("-1" + "\n");
                    break;
                }
                String outputFilepath = inputFilepath.substring(inputdir.length());
                String filepath = inputFilepath + " " + outputDir+"out_"+outputFilepath;
                outputStream.writeBytes(filepath + "\n");
                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String data = null;
                data = bufferedReader.readLine();
                if (data == null || data.equals("-1")) {
                    //handle dead condition
                    counter = 3;
                    while(counter > 0)
                    {
                        if(lock.tryLock())
                        {
                            try
                            {
                                Processing processing = Processing.getInstance();
                                Pending pending = Pending.getInstance();
                                processing.set.remove(inputFilepath);
                                pending.queue.offer(inputFilepath);
                                break;
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
                    System.out.println("Worker:" + workerID + " dead");
                }
                else {
                    counter = 3;
                    while(counter > 0)
                    {
                        if(lock.tryLock())
                        {
                            try
                            {
                                Processing processing = Processing.getInstance();
                                processing.set.remove(inputFilepath);
                                break;
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
            String lastInputFilepath = "";
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
                            InactiveWorkers inactiveWorkers = InactiveWorkers.getInstance();
                            activeWorkers.map.remove(workerID);
                            inactiveWorkers.set.add(workerID);
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
                    System.out.println("Worker: " + workerID);
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

