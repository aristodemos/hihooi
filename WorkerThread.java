package hih;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;

/**
 * Created by mariosp on 23/1/16.
 */
public class WorkerThread implements Callable<String>{

    private String url, user, pass;
    private static Transactions transactions;
    private static MarketThread market;
    WorkerThread(String url, String user, String pass, Transactions transactions, MarketThread market){
        this.url = url;
        this.pass = pass;
        this.user = user;
        this.transactions = transactions;
        this.market = market;
    }

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

            //TODO
            //Run tradeCleanup from a single thread.
            //Run trade Cleanup
            //DoTxn(stmt, "TradeCleanup");

            //Do Transaction
            List txnsToRun; //  = new Vector<String>();
            txnsToRun = hihUtil.workloadMix("d");
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
                    market.queue.put("MarketFeed|");
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
                        market.queue.put("TradeResult|"+trInput[0]+"|"+trInput[1]);
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }
                }
                break;
            case "TradeCleanup":
                transactions.tradeCleanup(st);
                break;
            default:
                System.out.println("Wrong txn type");
                break;
        }
    }

}
