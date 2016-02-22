package hih;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by mariosp on 6/2/16.
 */
public class hWorkerThread implements Callable<String> {

    private static hihTransactions transactions;
    //private  testTrans transactions;
    private static hMarketThread market;

    private hihUtil util;
    private int consistency_mode;
    private String workload_mix;

    hWorkerThread (hMarketThread market, hihTransactions txns, BenStatistics stats, int const_mode, String mix){
        this.transactions = txns;
        //this.transactions = new testTrans(stats);
        this.market = market;
        this.consistency_mode = const_mode;
        this.util = new hihUtil(stats);
        this.workload_mix = mix;
    }


    private volatile boolean running = true;

    public void terminate() {
        running = false;
        util.DISCONNECT();
        System.out.println("terminating wThread");
    }

    //@Override
    public String call() {
        util.CONNECT(); //System.out.println(util.CONNECT());
        util.setConsistency(consistency_mode);
        try{
            //Do Transaction
            List txnsToRun; //  = new Vector<String>();
            txnsToRun = hihUtil.workloadMix(workload_mix);
            int i=0;
            //int j=0;
            int numberOfTxns = txnsToRun.size();
            while(running){                       //while(j<2){  //while(i < numberOfTxns){
                DoTxn(util, txnsToRun.get(i).toString());
                i++;
                if (i==numberOfTxns){i=0;}
            }
            //Close Session
            util.DISCONNECT();
            //System.out.println("Thread " + Thread.currentThread().getName()+  " is finished. ");
            return "Thread " + Thread.currentThread().getName()+  " is finished. ";
        }
        catch(Exception e){
            e.printStackTrace();
            this.terminate();
            util.DISCONNECT();
            return "Thread " + Thread.currentThread().getName()+ " got Exception: " + e;
        }
    }

     void DoTxn(hihUtil util, String txnFrame){
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
                String trInput[]; // = new String[2];
                trInput = transactions.tradeOrder(util);
                if(trInput[0]!="" && trInput[1]!=""){
                    market.queue.add("TradeResult|"+trInput[0]+"|"+trInput[1]);
                }
                break;
            default:
                System.out.println("Wrong txn type");
                break;
        }
    }
}
