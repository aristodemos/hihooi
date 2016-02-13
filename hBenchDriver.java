package hih;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mariosp on 6/2/16.
 */
public class hBenchDriver {

    private static int      NUM_OF_THREADS      = 8;
    private static long     TIME_TO_RUN         = 1L;
    private static int      CONSISTENCY_MODE    = 1;
    private static String   WORKLOAD_MIX        = "a";

    static BenStatistics statistics = new BenStatistics();
    //hihTransactions transactions = new hihTransactions(statistics);


    public static void main (String args[]){
        hihSerializedData.initParams();
        Long startTime = System.currentTimeMillis();

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
            WORKLOAD_MIX = args[3];
            System.out.println("Consistency mode : " + CONSISTENCY_MODE);
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            System.out.println(dateFormat.format(date)); //2014/08/06 15:59:48
        }

        hMarketThread marketThread = new hMarketThread(CONSISTENCY_MODE, statistics);

        try{
            //Start Market Thread
            marketThread.start();
            System.out.println("Market Thread started");

            // Create Worker threads

            ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS);
            Collection<hWorkerThread> workerThreadsList = new ArrayList<>();

            for (int i = 0; i < NUM_OF_THREADS; i++) {
                workerThreadsList.add(new hWorkerThread(marketThread, CONSISTENCY_MODE, statistics, WORKLOAD_MIX));
            }

            List<Future<String>> listFut = pool.invokeAll(workerThreadsList, TIME_TO_RUN, TimeUnit.MINUTES);

            for (Future<String> f: listFut){
                try {
                    System.out.println("Session response "+f.get());
                }
                catch(Exception e) {
                    //e.printStackTrace();
                }
            }

            for (Iterator iterator = workerThreadsList.iterator(); iterator.hasNext();){
                hWorkerThread wt = (hWorkerThread) iterator.next();
                //String name = wt.toString();
                wt.terminate();
                //System.out.println("Terminating worker thread ... " + name);
            }
            pool.shutdownNow();
            while (!pool.isTerminated()) {
                //this can throw InterruptedException, you'll need to decide how to deal with that.
                pool.awaitTermination(1,TimeUnit.MILLISECONDS);
            }
            marketThread.terminate();
            marketThread.join();
        }
        catch(Exception e){
            e.printStackTrace();
            marketThread.terminate();
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
