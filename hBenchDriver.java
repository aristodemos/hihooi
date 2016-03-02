package hih;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;



/**
 * Created by mariosp on 6/2/16.
 */
public class hBenchDriver {

    private static int      NUM_OF_THREADS      = 2;
    private static long     TIME_TO_RUN         = 1L;
    private static int      CONSISTENCY_MODE    = 1;
    private static String   WORKLOAD_MIX        = "a";

    static BenStatistics statistics = new BenStatistics();
    static hihTransactions transactions = new hihTransactions(statistics);
    static BlockingQueue<String> bq = new LinkedBlockingQueue<String>();
    public static PrintWriter tpsLog = null;
    public static PrintWriter delaysLog = null;

    public static void main (String args[]){
        //initialize system metrics
        hihSerializedData.initParams();

        hihUtil initU = new hihUtil(statistics);
        initU.CONNECT();
        hihSerializedData.setNextSeq(Long.parseLong(initU.EXEC_QUERY("select max(t_id) from trade")));

        Long startTime = System.currentTimeMillis();

        File tps = new File ("tpsLog.txt");
        File delays = new File ("delays.txt");

        if (args.length > 4)
        {
            System.out.println("Error: Invalid Syntax. ");
            System.out.println("java BenchDriver [NoOfThreads] [TimeToRun] [MODE] [WORKLOAD]");
            System.exit(0);
        }

        // get the no of threads if given
        if (args.length > 0) {
            NUM_OF_THREADS = Integer.parseInt(args[0]);
            System.out.println("Number of Threads: " + NUM_OF_THREADS);

            TIME_TO_RUN = Long.parseLong(args[1]);
            System.out.println("Test will run for: " + TIME_TO_RUN + " minutes.");

            CONSISTENCY_MODE = Integer.parseInt(args[2]);
            System.out.println("Consistency Mode: " + CONSISTENCY_MODE);

            WORKLOAD_MIX = args[3];
            System.out.println("Workload Mix : " + WORKLOAD_MIX);

        }
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println(dateFormat.format(date)); //2014/08/06 15:59:48
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
            public void run() {
                tpsLog.print(statistics.totalTxns() + "\r\n");
            }
        }, 0, 5, TimeUnit.SECONDS);


        //hMarketThread marketThread1 = new hMarketThread(CONSISTENCY_MODE, statistics, bq);
        //hMarketThread marketThread2 = new hMarketThread(CONSISTENCY_MODE, statistics, bq);
        //hMarketThread marketThread = null;

        try{
            //Start Market Thread
            //marketThread1.start();marketThread2.start();

            // Create Worker threads
            Collection<Callable<String>> workerThreadsList = new ArrayList<>();
            for (int i = 0; i < NUM_OF_THREADS; i++) {
            //workerThreadsList.add(new hWorkerThread(marketThread,transactions,statistics,CONSISTENCY_MODE,WORKLOAD_MIX));
                workerThreadsList.add(new hWorkerThread(bq, transactions, statistics, CONSISTENCY_MODE,
                        WORKLOAD_MIX));
            }
            for (int i = 0; i < 4; i++) {
                workerThreadsList.add(new hMarketThreadC(CONSISTENCY_MODE, statistics, bq));
            }

            ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS+4);
            List<Future<String>> listFut = pool.invokeAll(workerThreadsList, TIME_TO_RUN, TimeUnit.MINUTES);

            for (Future<String> f: listFut){
                try {
                    System.out.println("Session response "+f.get());
                }catch(Exception e) {//e.printStackTrace();
                     }
            }
            for (Iterator iterator = workerThreadsList.iterator(); iterator.hasNext();){
                BenchThread wt = (BenchThread) iterator.next();
                String name = wt.toString();
                wt.terminate();
                System.out.println("Terminating thread ... " + name);
            }
            pool.shutdownNow();
            while (!pool.isTerminated()) {
                //this can throw InterruptedException, you'll need to decide how to deal with that.
                pool.awaitTermination(1,TimeUnit.MILLISECONDS);
            }
            exec.shutdownNow();
            while (!exec.isShutdown()){
                exec.awaitTermination(10L, TimeUnit.MILLISECONDS);
            }
        }
        catch(Exception e){
            e.printStackTrace();
            exec.shutdownNow();
            //marketThread1.terminate();marketThread2.terminate();
        }
        //Close the transaction Log File
        System.out.println("Closing Print Writers...");

        try {
            for (int k=0;k<statistics.groupSize;k++){
                delaysLog.print(statistics.txnPoolMaster.get(k).toString());
                delaysLog.println(statistics.groupDelays.get(k));
            }
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
