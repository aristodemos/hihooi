package hih;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by mariosp on 6/2/16.
 */
public class hWorkerThread implements Callable<String> {

    private static hihTransactions transactions;
    private static hMarketThread market;

    private hihUtil util = new hihUtil();
    private int consistency_mode;

    hWorkerThread(hihTransactions transactions, hMarketThread market, int const_mode ){
        this.transactions = transactions;
        this.market = market;
        this.consistency_mode = const_mode;
    }


    private volatile boolean running = true;

    public void terminate() {
        running = false;
    }

    //@Override
    public String call() throws Exception{
        System.out.println(util.CONNECT());
        util.setConsistency(consistency_mode);
        try{

            //Do Transaction
            List txnsToRun; //  = new Vector<String>();
            txnsToRun = hihUtil.workloadMix("d");
            int i=0;
            int numberOfTxns = txnsToRun.size();
            while(running){//while(i < numberOfTxns){
                DoTxn(util, txnsToRun.get(i).toString());
                i++;
                if (i==numberOfTxns)i=0;
            }

            // Close the statement
            util.DISCONNECT();

            //System.out.println("Thread " + Thread.currentThread().getName()+  " is finished. ");
            return "Thread " + Thread.currentThread().getName()+  " is finished. ";
        }
        catch(Exception e){
            e.printStackTrace();
            util.DISCONNECT();
            return "Thread " + Thread.currentThread().getName()+ " got Exception: " + e;
        }
    }

    static void DoTxn(hihUtil util, String txnFrame){
        switch (txnFrame){
            case "BrokerVolume":
                transactions.brokerVolumeFrame(util);
                break;
            case "CustomerPosition":
                transactions.customerPositionFrame(util);
                break;
            case "MarketFeed":
                //transactions.marketFeedFrame(st);
                try {
                    market.queue.put("MarketFeed|");
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
                break;
            case "TradeStatus":
                transactions.tradeStatus(util);
                break;
            case "SecurityDetail":
                transactions.securityDetail(util);
                break;
            case "TradeOrder":
                String trInput[] = new String[2];
                trInput = transactions.tradeOrder(util);
                if(trInput[0]!="" && trInput[1]!=""){
                    try {
                        market.queue.put("TradeResult|"+trInput[0]+"|"+trInput[1]);
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }
                }
                break;
            default:
                System.out.println("Wrong txn type");
                break;
        }
    }
}
