import java.io.IOException;
import java.lang.reflect.Field;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Comparator;

public class Utilities {

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


    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    public static String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }
    public static synchronized long getPidOfProcess(Process p) {
        long pid = -1;

        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }
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
