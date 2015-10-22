package hih;

/**
 * Created by mariosp on 22/10/15.
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.lang.ProcessBuilder;
import org.apache.commons.io.IOUtils;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
//import hih.org.json.JSONObject;

public class ExecuteShellCommand {

    public static void main(String[] args) {

        ExecuteShellCommand obj = new ExecuteShellCommand();

        String domainName = "google.com";

        //in mac oxs
        //String command = "ping -c 3 " + domainName;
        String query = "select * from broker";
        String output = obj.executeCommand(query);

        query = "select row_to_json(broker) from broker";
        output = obj.executeCommand(query);

        System.out.println(output);

    }
/*
    private String executeCommand2(String command){

        /*ProcessBuilder pb = new ProcessBuilder("myCommand", "myArg1", "myArg2");
        Map<String, String> env = pb.environment();
        env.put("VAR1", "myValue");
        env.remove("OTHERVAR");
        env.put("VAR2", env.get("VAR1") + "suffix");
        pb.directory(new File("myDir"));
        Process p = pb.start();

        StringBuffer output = new StringBuffer();
        ProcessBuilder pb = new ProcessBuilder("psql","-l");
        try {
            Process shell = pb.start();
            int error = shell.waitFor();
            shell.destroy();
            if (error == 0)
            {
                //pb = new ProcessBuilder("psql","-d dbt5", "-c 'select * from broker'");
                pb = new ProcessBuilder("psql","-d", "dbt5", "-c", "select * from broker");
                shell = pb.start();
                error = shell.waitFor();
                InputStream shellIn = shell.getInputStream();
                String response = IOUtils.toString(shellIn, "UTF-8");
                System.out.println(response);
                output.append(response);
                shellIn.close();
                shell.destroy();
                if (error != 0){
                    output.append("Errors");
                }
            }
        } catch (java.io.IOException ex) {
            System.out.println("failed");
            output.append("failed");
        } catch (InterruptedException ex) {
        }
        return output.toString();

    }
*/
    public static String executeCommand(String query) {

        StringBuffer output = new StringBuffer();

        Runtime rt = Runtime.getRuntime();
        String[] commands = new String[] {"psql", "-A", "-d", "tpc", "-U", "postgres", "-h", "prm1", "-c", query};

        try {
            Process proc = rt.exec(commands);

            BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));

            BufferedReader stdError = new BufferedReader(new
                InputStreamReader(proc.getErrorStream()));

            // read the output from the command
            //System.out.println("Here is the standard output of the command:\n");
            String s = null;
            while ((s = stdInput.readLine()) != null) {
                //System.out.println(s);
                output.append(s);
            }

            // read any errors from the attempted command
            //System.out.println("Here is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
                //System.out.println(s);
                output.append(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return output.toString();

    }
}
