package hih;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.*;
//import java.io.FileNotFoundException;
//import java.io.UnsupportedEncodingException;
import java.io.PrintWriter;

/**
 * Created by mariosp on 23/1/16.
 */
public class BenchDriver {

    private static int  NUM_OF_THREADS  = 8;
    private static long TIME_TO_RUN     = 20L;
    private static String WORKLOAD_MIX  = "d";
    static Connection s_conn = null;
    static  boolean   share_connection = false;
    //static String url = "jdbc:postgresql://dicl09.cut.ac.cy/tpce";
    static String url = "jdbc:postgresql://localhost/tpce";
    static String user = "mariosp";
    static String pass = "";

    static BenStatistics statistics = new BenStatistics();
    static Transactions transactions = new Transactions(statistics);
    //static testTransj trans = new testTransj(statistics);

    static BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

    public static PrintWriter tpsLog = null;
    public static PrintWriter delaysLog = null;

    public static void main (String args[]){
        hihSerializedData.initParams();
        Long startTime = System.currentTimeMillis();

        File tps = new File ("tpsLog.txt");
        File delays = new File ("delays.txt");
        //TODO: Create Market Thread
        //Market Threads are now in the same pool as the worker threads
        //just remember to extend BenchThread class


        try{
            /* Load the JDBC driver */
            // If NoOfThreads is specified, then read it
            if (args.length > 3)
            {
                System.out.println("Error: Invalid Syntax. ");
                System.out.println("java BenchDriver [NoOfThreads] [TimeToRun] [WORKLOAD_MIX]");
                System.exit(0);
            }

            // get the no of threads if given
            if (args.length > 0) {
                NUM_OF_THREADS = Integer.parseInt(args[0]);
                System.out.println("Number of Threads: " + NUM_OF_THREADS);
                TIME_TO_RUN = Long.parseLong(args[1]);
                WORKLOAD_MIX = args[2];
                System.out.println("Test will run for: " + TIME_TO_RUN + " minutes.");
            }
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            System.out.println(dateFormat.format(date)); //2014/08/06 15:59:48

            //TODO: Create Directory to save results
            //Create transaction Log File
            //Remember to close it
            String timeStamp = dateFormat.format(date);
            try{
                delaysLog   = new PrintWriter(delays);
                tpsLog      = new PrintWriter(tps);
            }catch (FileNotFoundException err){
                err.printStackTrace();
                System.out.println("FileNOTfound " + err.getMessage());
            }
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            exec.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {tpsLog.print(statistics.totalTxns() + "\r\n");}
            }, 0, 5, TimeUnit.SECONDS);

            // Create Worker threads
            Collection<Callable<String>> workerThreadsList = new ArrayList<>();
            for (int i = 0; i < NUM_OF_THREADS; i++) {
                workerThreadsList.add(new WorkerThread(url, user, pass, transactions, queue, WORKLOAD_MIX));
            }
            for (int i = 0; i < 2; i++) {
                workerThreadsList.add(new MarketThreadC(url, user, pass, transactions, queue));
            }

            ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS+2);
            List<Future<String>> listFut = pool.invokeAll(workerThreadsList, TIME_TO_RUN, TimeUnit.MINUTES);

            for (Future<String> f: listFut){
                try {
                    System.out.println("Session response "+f.get());
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }


            for (Iterator iterator = workerThreadsList.iterator(); iterator.hasNext();){
                BenchThread wt = (BenchThread) iterator.next();
                String name = wt.toString();
                wt.terminate();
                System.out.println("Terminating worker thread ... " + name);
            }

            pool.shutdownNow();
            exec.shutdownNow();
            while (!exec.isShutdown()){
                exec.awaitTermination(10L, TimeUnit.MILLISECONDS);
            }
            //TODO: Single Market Thread
            //marketThread.terminate();
            //marketThread.join();
        }
        catch(Exception e){
            e.printStackTrace();
            //marketThread.terminate();
        }

        //Close the transaction Log File
        System.out.println("Closing Print Writers...");

        try {
            for (int k=0;k<statistics.groupSize;k++){
                delaysLog.print(statistics.txnPoolMaster.get(k).toString());
                delaysLog.println(statistics.groupDelays.get(k));
            }
            /*
            delaysLog.print(statistics.txnPoolMaster.get(1).toString());
            delaysLog.println(statistics.groupDelays.get(1));
            delaysLog.print(statistics.txnPoolMaster.get(2).toString());
            delaysLog.println(statistics.groupDelays.get(2));
            delaysLog.print(statistics.txnPoolMaster.get(3).toString());
            delaysLog.println(statistics.groupDelays.get(3));
            delaysLog.print(statistics.txnPoolMaster.get(0).toString());
            delaysLog.println(statistics.groupDelays.get(0));
            delaysLog.print(statistics.txnPoolMaster.get(4).toString());
            delaysLog.println(statistics.groupDelays.get(4));
            delaysLog.print(statistics.txnPoolMaster.get(5).toString());
            delaysLog.println(statistics.groupDelays.get(5));
            delaysLog.print(statistics.txnPoolMaster.get(6).toString());
            delaysLog.println(statistics.groupDelays.get(6));
            */
            if (!tpsLog.equals(null)) tpsLog.close();
            if (!delaysLog.equals(null)) delaysLog.close();
        }catch (Exception e){e.printStackTrace();}

        Long duration = System.currentTimeMillis() - startTime;
        System.out.println("#############################################################");
        System.out.println("####################   Test Results   #######################");
        System.out.println("#");
        System.out.println("#\tNumber of Worker Threads: \t\t"+NUM_OF_THREADS);
        System.out.println("#\tTotal Number of Transactions ran: \t" + statistics.totalTxns());
        System.out.println("#\tTotal Time (in seconds): \t\t" + duration / 1000.0);

        String result = String.format(Locale.US, "#\tTransactions Per Second: \t\t%.3f tps",statistics
                .totalTxns() /
                (duration / 1000.0));
        System.out.println(result);
        System.out.println("#\tTotal Number of Operations: \t\t" + statistics.totalOps());
        System.out.println("#\tTotal Number Writes : \t\t\t" + statistics.totalWriteOps());
        /////
        System.out.println("*********************Test Run statistics********************");
        System.out.println("************************************************************");
        System.out.println("*Txn Mix:");
        System.out.println("*BrokerVolume Txn was run: \t\t"+    statistics.txnMix[0] + " times;");
        System.out.println("*CustomerPosition Txn was run: \t\t"+  statistics.txnMix[1] + " times;");
        System.out.println("*MarketFeed Txn was run: \t\t"+    statistics.txnMix[2] + " times;");
        System.out.println("*TradeOrder Txn was run: \t\t"+      statistics.txnMix[3] + " times;");
        System.out.println("*TradeResult Txn was run: \t\t"+     statistics.txnMix[4] + " times;");
        System.out.println("*TradeStatus Txn was run: \t\t"+     statistics.txnMix[5] + " times;");
        System.out.println("*SecurityDetail Txn was run: \t\t"+  statistics.txnMix[6] + " times;");

        System.out.println("\n\n****************** Txn Duration (in msec) ******************");
        System.out.println("************************************************************");
        String res = String.format(Locale.US,"%.3f",((double)statistics.txnDuration[0])/statistics.txnMix[0]);
        System.out.println("*Broker Volume avg time\t\t: "  +res);
        res = String.format(Locale.US, "%.3f",((double)statistics.txnDuration[1])/statistics.txnMix[1]);
        System.out.println("*Customer Position avg time\t: "+res);
        res = String.format(Locale.US, "%.3f",((double)statistics.txnDuration[2])/statistics.txnMix[2]);
        System.out.println("*Market Feed avg time\t\t: "    +res);
        res = String.format(Locale.US, "%.3f",((double)statistics.txnDuration[3])/statistics.txnMix[3]);
        System.out.println("*Trade Order avg time\t\t: "    +res);
        res = String.format(Locale.US, "%.3f",((double)statistics.txnDuration[4])/statistics.txnMix[4]);
        System.out.println("*Trade Result avg rime\t\t: "   +res);
        res = String.format(Locale.US, "%.3f",((double)statistics.txnDuration[5])/statistics.txnMix[5]);
        System.out.println("*Trade Status avg rime\t\t: "   +res);
        res = String.format(Locale.US, "%.3f",((double)statistics.txnDuration[6])/statistics.txnMix[6]);
        System.out.println("*Security Detail avg rime\t: "+res);

    }
}
