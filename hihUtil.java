package hih;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;

import hih.BenStatistics;

/**
 * Created by mariosp on 31/1/16.
 */
public class hihUtil {

    BenStatistics stats;// = new BenStatistics();
    public static PrintWriter logWriter = null;
    private static long nextSeq = 200000000999999L;
    private static boolean  DEBUG   = false; //print transactions to file and other msgs on system.out

    hihUtil(BenStatistics s){
        this.stats = s;
        String timeStamp = new SimpleDateFormat("yy.MM.dd.HH.mm.ss").format(new java.util.Date());
        if (DEBUG){
            try{
                logWriter = new PrintWriter("LOG_"+timeStamp+".txt", "UTF-8");
            }catch (FileNotFoundException |UnsupportedEncodingException err){
                System.out.println("FileNOTfound" + err.getMessage());
            }
        }
    }

    public static Random testRndGen = new Random();



    //private String LISTENER="dicl";
    private String LISTENER="dicl09.cut.ac.cy";
    private HiHListenerClient hih = new HiHListenerClient();

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

    public String DISCONNECT() {
        if(DEBUG)logWriter.close();
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
        if (DEBUG ){logWriter.printf("%s \n", SQL);}
        List<Map<String, Object>> rows = hih.executeQuery(SQL);
        String output ="";
        for( int i = rows.size() -1; i >= 0 ; i --) {
            Map<String,Object> entry = rows.get(i);
            for (String key : entry.keySet()) {
                output = ""+entry.get(key);
            }
        }
        if (DEBUG){logWriter.println(output);}
        stats.incOperation();
        return output;
    }

    public List QUERY(String SQL) {
        if (DEBUG ){logWriter.printf("%s \n", SQL);}
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
        if (DEBUG){logWriter.println(resultOut);}
        stats.incOperation();
        return resultOut;
    }

    public Map QUERY2MAP(String SQL){
        if (DEBUG ){logWriter.printf("%s \n", SQL);}
        List<Map<String, Object>> rows = hih.executeQuery(SQL);
        Map<String, Object> results = new HashMap<>();
        for( int i = rows.size() -1; i >= 0 ; i --) {
            Map<String,Object> entry = rows.get(i);
            for (String key : entry.keySet()){
                results.put(key, entry.get(key));
            }
        }
        if (DEBUG){logWriter.println(results);}
        stats.incOperation();
        return results;
    }

    public List QUERY2LST(String SQL){
        if (DEBUG ){logWriter.printf("%s \n", SQL);}
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
        if (DEBUG){logWriter.println(rows);}
        stats.incOperation();
        return rows;
    }

    public String DML(String SQL) {
        if (DEBUG ){logWriter.printf("%s \n", SQL);}
        stats.incWriteOp();
        return hih.executeUpdate(SQL);
    }

    public String START_TX() {
        //stats.incWriteOp();
        //if (DEBUG ){logWriter.printf("START_TRANSACTION \n");}
        return hih.startTransaction();
    }

    //TRANSACTION CONTROL LANGUAGE: COMMIT, ROLLBACK;
    public String TCL(String tcl_cmd) {
        //if (DEBUG ){logWriter.printf("%s \n", tcl_cmd);}
        if (tcl_cmd.equalsIgnoreCase("commit")) {
            stats.incOperation();
            return hih.commitTransaction();
        }
        else {
            stats.incOperation();
            return hih.rollbackTransaction();
        }
    }

    public String getNextSeqNumber(){
        return Long.toString(nextSeq++);
    }

    public void setNextSeq(long next){
        nextSeq=next+1;
        System.out.println("SET SEQUENCE NUMBER to "+nextSeq);
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

    public static List workloadMix(String selector) {
        List<String> pool = new Vector<String>();
        switch(selector) {
            case "z":
                for (int i=0; i<50;i++){
                    pool.add("BrokerVolume");
                }
                for (int i=50; i<90;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=90; i<91;i++){
                    pool.add("MarketFeed");
                }
                for (int i=91;i<100;i++){
                    pool.add("SecurityDetail");
                }
                return randomSample(pool, pool.size());
            case "a":
                for (int i=0; i<5;i++){
                    pool.add("BrokerVolume");
                }
                for (int i=5; i<18;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=18;i<34;i++){
                    pool.add("SecurityDetail");
                }
                for (int i=34;i<62;i++){
                    pool.add("TradeStatus");
                }
                for (int i=62;i<70;i++){
                    pool.add("TradeStatus");
                }
                for (int i=70;i<80;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=80;i<100;i++){
                    pool.add("BrokerVolume");
                }
                return randomSample(pool, pool.size());
            case "b":
                for (int i=0; i<5;i++){
                    pool.add("BrokerVolume");
                }
                for (int i=5; i<18;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=18;i<33;i++){
                    pool.add("SecurityDetail");
                }
                for (int i=33;i<34;i++){
                    pool.add("TradeOrder");
                }
                for (int i=34;i<62;i++){
                    pool.add("TradeStatus");
                }
                for (int i=62;i<70;i++){
                    pool.add("TradeStatus");
                }
                for (int i=70;i<80;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=80;i<100;i++){
                    pool.add("BrokerVolume");
                }
                return randomSample(pool, pool.size());
            case "c":
                for (int i=0; i<5;i++){
                    pool.add("BrokerVolume");
                }
                for (int i=5; i<18;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=18;i<33;i++){
                    pool.add("SecurityDetail");
                }
                for (int i=33;i<36;i++){
                    pool.add("TradeOrder");
                }
                for (int i=36;i<62;i++){
                    pool.add("TradeStatus");
                }
                for (int i=62;i<70;i++){
                    pool.add("TradeStatus");
                }
                for (int i=70;i<80;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=80;i<100;i++){
                    pool.add("BrokerVolume");
                }
                return randomSample(pool, pool.size());
            //case 'd' is the default
            case "f":
                for (int i=0; i<5;i++){
                    pool.add("BrokerVolume");
                }
                for (int i=5; i<18;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=18;i<33;i++){
                    pool.add("SecurityDetail");
                }
                for (int i=33;i<34;i++){
                    pool.add("TradeOrder");
                }
                for (int i=34;i<62;i++){
                    pool.add("TradeStatus");
                }
                for (int i=62;i<70;i++){
                    pool.add("TradeStatus");
                }
                for (int i=70;i<80;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=80;i<100;i++){
                    pool.add("BrokerVolume");
                }
                return randomSample(pool, pool.size());

            default:
                for (int i=0; i<5;i++){
                    pool.add("BrokerVolume");
                }
                for (int i=5; i<18;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=18; i<19;i++){
                    pool.add("MarketFeed");
                }
                for (int i=19;i<33;i++){
                    pool.add("SecurityDetail");
                }
                for (int i=33;i<43;i++){
                    pool.add("TradeOrder");
                }
                for (int i=43;i<62;i++){
                    pool.add("TradeStatus");
                }
                for (int i=62;i<70;i++){
                    pool.add("TradeStatus");
                }
                for (int i=70;i<80;i++){
                    pool.add("CustomerPosition");
                }
                for (int i=80;i<90;i++){
                    pool.add("BrokerVolume");
                }
                return randomSample(pool, pool.size());

        }
    }

}
