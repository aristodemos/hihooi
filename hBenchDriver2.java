package hih;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by mariosp on 10/2/16.
 */
public class hBenchDriver2 {

    private static int      NUM_OF_THREADS      = 4;
    private static long     TIME_TO_RUN         = 5L;
    private static int      CONSISTENCY_MODE    = 1;
    private static String   WORKLOAD_MIX        = "f";
    static BenStatistics statistics = new BenStatistics();
    static hihTransactions transactions = new hihTransactions(statistics);

    public static void main (String args[]){

        hihSerializedData.initParams();
        Long startTime = System.currentTimeMillis();

        hMarketThread marketThread = new hMarketThread(transactions, CONSISTENCY_MODE, statistics);
        marketThread.start();
        System.out.println("Market Thread started");


        try{
            ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS);
            List<Future<String>> list = new ArrayList<Future<String>>();

            Callable<String> callable = new hWorkerThread(transactions, marketThread, CONSISTENCY_MODE, statistics, WORKLOAD_MIX);
            //Callable<String> callable = new  TestHihooiDriver();
            for(int i=0; i< NUM_OF_THREADS; i++) {
                //submit Callable tasks to be executed by thread pool
                Future<String> future = pool.submit(callable);
                //add Future to the list, we can get return value using Future
                list.add(future);
            }
            for(Future<String> fut : list)
            {
                try {
                    System.out.println("Session response "+fut.get());
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            pool.shutdownNow();
            marketThread.terminate();
            marketThread.join();
            while (!pool.isTerminated()) {
                //this can throw InterruptedException, you'll need to decide how to deal with that.
                pool.awaitTermination(1,TimeUnit.MILLISECONDS);
            }
        }
        catch(Exception e) {
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
