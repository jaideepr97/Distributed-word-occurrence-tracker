import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Comparator;

public class Utilities {

    //Custom comparator for sorting the words
    public static class CustomComparator implements Comparator<String[]>
    {
        public int compare(String[] a, String[] b)
        {
            if(Integer.parseInt(a[1]) == Integer.parseInt(b[1]))
            {
                return a[0].compareTo(b[0]);
            }
            else
            {
                return Integer.parseInt(b[1]) - Integer.parseInt(a[1]);
            }
        }
    }
    //A test method which was used for unit testing
    public static boolean test(int socket, int workerID, String inputFilepath, String outFilepath) throws IOException
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
    //A utility method to check available ports
    public static boolean checkPortAvailability(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }
}
