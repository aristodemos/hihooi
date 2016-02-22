package hih;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by mariosp on 6/2/16.
 */
public class hMarketThread extends Thread{
    //List<String> objs = new ArrayList<String>();//init it
    public BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

    hihTransactions transactions;
    private hihUtil util;// = new hihUtil();

    hMarketThread(int consistency_mode, BenStatistics stats){
        this.transactions = new hihTransactions(stats);
        this.util = new hihUtil(stats);
        System.out.println("Market Thread: " + util.CONNECT());
        util.setConsistency(consistency_mode);
        util.setNextSeq(Long.parseLong(util.EXEC_QUERY("select max(t_id) from trade") + "L"));
    }

    private volatile boolean running = true;

    public void terminate() {
        running = false;
        util.DISCONNECT();
        System.out.println("Market Thead Stopped");
    }

    public void run(){
        while(running){
            String msg;
            if (queue.size() < 1){Thread.currentThread().setPriority(Thread.NORM_PRIORITY);}
            while ((msg = queue.poll()) != null){
                //System.out.println("_"+Thread.currentThread().getName() +  "received msg: " + msg);
                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                DoMarketTxn(msg);
            }
        }
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
