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
    private hihUtil util = new hihUtil();

    hMarketThread(hihTransactions trans, int consistency_mode){
        this.transactions = trans;
        util.CONNECT();
        util.setConsistency(consistency_mode);
    }

    private volatile boolean running = true;

    public void terminate() {
        running = false;
    }

    public void run(){
        while(running){
            String msg;
            while ((msg = queue.poll()) != null){
                //System.out.println("_"+Thread.currentThread().getName() +  "received msg: " + msg);
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
