package hih;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by mariosp on 23/1/16.
 */
public class BenchDriver {

    private static int  NUM_OF_THREADS  = 16;
    private static long TIME_TO_RUN     = 1L;
    private static String WORKLOAD_MIX  = "d";
    static Connection s_conn = null;
    static  boolean   share_connection = false;
    static String url = "jdbc:postgresql://localhost/tpce";
    static String user = "postgres";
    static String pass = "";

    static BenStatistics statistics = new BenStatistics();
    static Transactions transactions = new Transactions(statistics);

    public static void main (String args[]){
        hihSerializedData.initParams();
        Long startTime = System.currentTimeMillis();

        //TODO: Create Market Thread
        //Market Threads executed all MarketFeed and TradeResult Transactions;
        //The rest are handled by the WorkerThreads
        MarketThread marketThread = new MarketThread(url, user, pass, transactions);
        System.out.println("Market Thread started");
        marketThread.start();

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
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println(dateFormat.format(date)); //2014/08/06 15:59:48
            }

            // Create Worker threads
            Collection<WorkerThread> workerThreadsList = new ArrayList<>();
            for (int i = 0; i < NUM_OF_THREADS; i++) {
                workerThreadsList.add(new WorkerThread(url, user, pass, transactions, marketThread, WORKLOAD_MIX));
            }

            ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS);
            List<Future<String>> listFut = pool.invokeAll(workerThreadsList, TIME_TO_RUN, TimeUnit.MINUTES);
            for (Iterator iterator = workerThreadsList.iterator(); iterator.hasNext();){
                WorkerThread wt = (WorkerThread) iterator.next();
                String name = wt.toString();
                wt.terminate();
                System.out.println("Terminating worker thread ... " + name);
            }
            pool.shutdownNow();
            marketThread.terminate();
            marketThread.join();
        }
        catch(Exception e){
            e.printStackTrace();
        }

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
