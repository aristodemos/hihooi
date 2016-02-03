package hih;

import java.util.*;

import hih.BenStatistics;

/**
 * Created by mariosp on 31/1/16.
 */
public class hihUtil {

    static  BenStatistics stats;// = new BenStatistics();

    public static Random testRndGen = new Random();

    private static boolean  DEBUG           = false; //print transactions to file and other msgs on system.out

    //private String LISTENER="52.27.159.172";
    private String LISTENER="172.30.0.130";
    private static HiHListenerClient hih = new HiHListenerClient();

    public String CONNECT() {
        Properties p = new Properties();
        p.setProperty("server",this.LISTENER);
        p.setProperty("port","7777");
        p.setProperty("username","user01");
        p.setProperty("password","12345678");
        p.setProperty("identifier","client01");
        p.setProperty("service_name","TESTSRV");
        return hih.connect(p);   /// Retun SESSION-ID e.g  21703567-1ed7-4f59-aeac-39686ea9c2b1
    }

    public static String DISCONNECT()
    {
        return hih.disconnect();  //Returns "Disconnect Successfuly"
    }

    public String setConsistency(int setCMD) {
		/*
		hih.set(“set consistency level 1”)etc.
			set consistency level 2;
			set consistency level 3;
			set consistency level 4;
		*/
        return hih.set("set consistency level " + setCMD);
    }

    public String EXEC_QUERY(String SQL) {
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
        List<Map<String, Object>> rows = hih.executeQuery(SQL);
        String output ="";
        for( int i = rows.size() -1; i >= 0 ; i --) {
            Map<String,Object> entry = rows.get(i);
            for (String key : entry.keySet()) {
                output = ""+entry.get(key);
            }
        }
        if (DEBUG){System.out.println(output);}
        stats.incOperation();
        return output;
    }

    public static List QUERY(String SQL) {
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
        List<Map<String, Object>> rows = hih.executeQuery(SQL);
        String output ="";
        List resultOut = new Vector();
        for( int i = rows.size() -1; i >= 0 ; i --)
        {
            Map<String,Object> entry = rows.get(i);
            for (String key : entry.keySet())
            {
                output += entry.get(key)+" ";
                resultOut.add(entry.get(key));
            }
        }
        if (DEBUG){System.out.println(resultOut);}
        stats.incOperation();
        return resultOut;
    }

    public Map QUERY2MAP(String SQL){
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
        List<Map<String, Object>> rows = hih.executeQuery(SQL);
        Map<String, Object> results = new HashMap<>();
        for( int i = rows.size() -1; i >= 0 ; i --) {
            Map<String,Object> entry = rows.get(i);
            for (String key : entry.keySet()){
                results.put(key, entry.get(key));
            }
        }
        if (DEBUG){System.out.println(results);}
        stats.incOperation();
        return results;
    }

    public List QUERY2LST(String SQL){
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
        List<Map<String, Object>> rows = hih.executeQuery(SQL);
        String output ="";
        for( int i = rows.size() -1; i >= 0 ; i --)
        {
            Map<String,Object> entry = rows.get(i);
            for (String key : entry.keySet())
            {
                output += entry.get(key);
            }
        }
        if (DEBUG){System.out.println(rows);}
        stats.incOperation();
        return rows;
    }

    public String DML(String SQL) {
        //if (DEBUG){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
        stats.incWriteOp();
        return hih.executeUpdate(SQL);
    }

    public String START_TX() {
        //stats.incWriteOp();
        return hih.startTransaction();
    }
    //TRANSACTION CONTROL LANGUAGE: COMMIT, ROLLBACK;
    public String TCL(String tcl_cmd) {
        if (tcl_cmd.equalsIgnoreCase("commit")) {
            stats.incOperation();
            return hih.commitTransaction();
        }
        else {
            stats.incOperation();
            return hih.rollbackTransaction();
        }
    }

    //random sampling - a bounded version of Knuth's algorithm
    //taken from https://eyalsch.wordpress.com/2010/04/01/random-sample/
    public static <T> List<T> randomSample(List<T> items, int m){
        for(int i=0;i<m;i++){
            //int pos = i + ThreadLocalRandom.current().nextInt(items.size() - i);
            int pos = i + testRndGen.nextInt(items.size() - i);
            T tmp = items.get(pos);
            items.set(pos, items.get(i));
            items.set(i, tmp);
        }
        return items.subList(0, m);
    }

}
