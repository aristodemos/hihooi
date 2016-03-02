package hih;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * Created by mariosp on 2/3/16.
 */
public class hMarketThreadC extends BenchThread implements Callable<String> {

    BlockingQueue<String> queue;
    hihTransactions transactions;
    private hihUtil util;

    hMarketThreadC(int consistency_mode, BenStatistics stats, BlockingQueue<String> bq){
        this.transactions = new hihTransactions(stats);
        this.queue = bq;
        this.util = new hihUtil(stats);
        System.out.println("Market Thread C: " + util.CONNECT());
        util.setConsistency(consistency_mode);

    }

    private volatile boolean running = true;

    public void terminate() {
        running = false;
        util.DISCONNECT();
        System.out.println("Market Thread C Stopped");
    }

    public String call(){
        String msg;
        util.setNextSeq(Long.parseLong(util.EXEC_QUERY("select max(t_id) from trade")));
        while(running){
            while ((msg = queue.poll()) != null){
                //System.out.println("_"+Thread.currentThread().getName() +  "received msg: " + msg);
                DoMarketTxn(msg);
            }
            //System.out.println("Q : : : : :" + queue);
        }
        return "Market Thread Finished!";
    }

    void DoMarketTxn(String txnFrame){
        String[] parts = txnFrame.split("\\|");
        switch (parts[0]){
            case "MarketFeed":
                transactions.marketFeedFrame(util);
                break;
            case "TradeResult":
                transactions.tradeResult(util, parts[1], Double.parseDouble(parts[2]));
                break;
            default:
                System.out.println("Wrong Market Txn type");
                System.out.println(parts[0]);
                break;
        }
    }
}
