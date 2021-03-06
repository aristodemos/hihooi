package hih;

import java.awt.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * Created by mariosp on 23/1/16.
 */
public class WorkerThread extends BenchThread  implements Callable<String>{

    private String url, user, pass;
    private static Transactions transactions;
    //private static testTransj transactions_limited;
    //private static MarketThread market;
    private static BlockingQueue<String> queue;
    private static String workload_mix;

    WorkerThread(String url, String user, String pass, Transactions transactions, MarketThread market, String mix){
        this.url = url;
        this.pass = pass;
        this.user = user;
        this.transactions = transactions;
        //this.market = market;
        //this.queue=market.queue;
        this.workload_mix = mix;
    }

    WorkerThread(String url, String user, String pass, Transactions transactions, BlockingQueue<String> bq, String mix){
        this.url = url;
        this.pass = pass;
        this.user = user;
        this.transactions = transactions;
        this.queue=bq;
        this.workload_mix = mix;
    }

    /*
    WorkerThread(String url, String user, String pass, testTransj transactions, MarketThread market, String mix){
        this.url = url;
        this.pass = pass;
        this.user = user;
        this.transactions = transactions;
        this.market = market;
        this.workload_mix = mix;
    }*/

    private volatile boolean running = true;

    public void terminate() {
        running = false;
    }

    //@Override
    public String call() throws Exception{
        Connection conn    = null;
        Statement stmt    = null;
        try{
            // Get the connection
            conn = DriverManager.getConnection(url, user, pass);
            stmt = conn.createStatement(); // Create a Statement

            //Do Transaction
            List txnsToRun; //  = new Vector<String>();
            txnsToRun = hihUtil.workloadMix(workload_mix);
            int i=0;
            int numberOfTxns = txnsToRun.size();
            while(running){//while(i < numberOfTxns){
                DoTxn(stmt, txnsToRun.get(i).toString());
                i++;
                if (i==numberOfTxns)i=0;
            }
            // Close the statement
            stmt.close();
            stmt = null;
            //System.out.println("Thread " + Thread.currentThread().getName()+  " is finished. ");
            return "Thread " + Thread.currentThread().getName()+  " is finished. ";
        }
        catch(Exception e){
            e.printStackTrace();
            return "Thread " + Thread.currentThread().getName()+ " got Exception: " + e;
        }
    }

    static void DoTxn(Statement st, String txnFrame){
        switch (txnFrame){
            case "BrokerVolume":
                transactions.brokerVolumeFrame(st);
                break;
            case "CustomerPosition":
                transactions.customerPositionFrame(st);
                break;
            case "MarketFeed":
                //transactions.marketFeedFrame(st);
                try {
                    queue.put("MarketFeed|");

                }catch(InterruptedException e){
                    e.printStackTrace();
                }
                break;
            case "TradeStatus":
                transactions.tradeStatus(st);
                break;
            case "SecurityDetail":
                transactions.securityDetail(st);
                break;
            case "TradeOrder":
                String trInput[] = new String[2];
                trInput = transactions.tradeOrder(st);
                if(trInput[0]!="" && trInput[1]!=""){
                    try {
                        queue.put("TradeResult|"+trInput[0]+"|"+trInput[1]);
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
