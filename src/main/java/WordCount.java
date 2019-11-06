import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.LinkedList;


public class WordCount implements Master, Runnable{
    public WordCount(int workerNum, String[] filenames) throws IOException {

    }

    public void setOutputStream(PrintStream out) {

    }

    public static void main(String[] args) throws Exception {

        //WordCount w = new WordCount(1, new String[]{});
        //w.createWorker();
        ProcessBuilder temp  = new ProcessBuilder(new String[]{"java", "Worker"});
        Process p = Runtime.getRuntime().exec("java Worker");
    }

    public void run() {



    }

    public Collection<Process> getActiveProcess() {
        return new LinkedList<>();
    }
    private static final String JAVA_FILE_LOCATION = "/Users/aayushgupta/IdeaProjects/project-2-group-2/src/main/java/";

    public void createWorker() throws IOException {

        try
        {
            String command[] = {"javac", JAVA_FILE_LOCATION+"Worker.java"};
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            Process process = processBuilder.start();
/**
 * Check if any errors or compilation errors encounter then print on Console.
 */

            if( process.getErrorStream().read() != -1 ){
                System.out.println("Compilation Errors"+process.getErrorStream());
            }
/**
 * Check if javac process execute successfully or Not
 * 0 - successful
 */
            process.waitFor();
            if( process.exitValue() == 0 ) {
                ProcessBuilder temp  = new ProcessBuilder(new String[]{"java", JAVA_FILE_LOCATION+"Worker"});
                Process p = temp.start();
            }
        }
        catch (InterruptedException e)
        {
            System.out.println(e.getStackTrace());
        }


    }
}

