import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.io.*;

public class WordCount implements Master{

    HashMap<Integer, String> map;
    HashSet<String> pending;
    HashSet<String> complete;
    HashSet<String> processing;
    List<String> total;
    boolean[] workers;
    private static final String outputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    static String inputDir = "/Users/aayushgupta/IdeaProjects/project-2-group-2/tests/";
    private static final String JAVA_FILE_LOCATION = "/Users/aayushgupta/IdeaProjects/project-2-group-2/src/main/java/";
    public WordCount(int workerNum, String[] filenames) throws IOException {

        workers = new boolean[workerNum];
        Arrays.fill(workers, false);
        map = new HashMap();
        pending = new HashSet<>();
        total = new LinkedList<>();
        for(String s : filenames)
        {
            total.add(s);
            pending.add(s);
        }
        complete = new HashSet<>();
        processing = new HashSet();
    }

    public void setOutputStream(PrintStream out) {

    }

    public static void main(String[] args) throws Exception {
        WordCount w = new WordCount(2, new String[]{inputDir+"king-james-version-bible.txt"
                ,inputDir+"war-and-peace.txt"});
        if(w.compileWorker() == 0)
        {
            w.createWorker();
        }
    }

    public void run() {




    }

    public Collection<Process> getActiveProcess() {
        return new LinkedList<>();
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
            MasterHeartbeat hb = new MasterHeartbeat(0, 50001);
            Thread heartBeatThread = new Thread(hb);
            heartBeatThread.start();
            String[] command = new String[]{"java", "Worker", "0", "50001", "55001"};
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.directory(new File(JAVA_FILE_LOCATION)).inheritIO();
            Process process = processBuilder.start();
            //process.waitFor();
            //Process p = Runtime.getRuntime().exec("java Worker 0 50001")
            /*
            Thread.sleep(10000);
            process.destroy();
            if(process.isAlive())
                process.destroyForcibly();
             */
            this.test(55001, 0, inputDir+"king-james-version-bible.txt", outputDir+"out.txt");

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
class MasterHeartbeat implements Runnable
{
    int socket;
    int workerID;
    public MasterHeartbeat(int _workerID, int _socket)
    {
        socket = _socket;
        workerID = _workerID;
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
            while(true){
                Thread.sleep(3000);
                bfr = new BufferedReader(new InputStreamReader(scc.getInputStream()));
                String data = null;
                data = bfr.readLine();
                if(data == null)
                {
                    //handle dead condition
                    System.out.println("Worker:"+ workerID + " dead");
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

