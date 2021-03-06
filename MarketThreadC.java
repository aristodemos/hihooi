package hih;

import java.sql.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * Created by mariosp on 28/2/16.
 */
public class MarketThreadC extends BenchThread implements Callable<String> {

    BlockingQueue<String> queue ;//= new LinkedBlockingQueue<String>();
    String url, pass, user;
    static Transactions transactions;
    Connection conn;
    Statement statement;

    MarketThreadC(String url ,String user, String pass, Transactions trans, BlockingQueue<String> bq){
        this.transactions = trans;
        this.url = url;
        this.pass = pass;
        this.user = user;
        this.queue = bq;
        try{
            conn = DriverManager.getConnection(url, user, pass);
            this.statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("select max(t_id) from trade");
            if (rs.next()){
                transactions.setNextSeq(rs.getLong("max"));
            }

        }
        catch (SQLException e){e.printStackTrace();}
    }

    private volatile boolean running = true;

    public void terminate() {
        running = false;
        System.out.println("Market Thead Stopped");
        queue = null;
        try{
            conn.rollback();
            conn.close();}
        catch (SQLException e){e.printStackTrace();}

    }

    public String call(){
        String msg;
        while(running){
            while ((msg = queue.poll()) != null){
                //System.out.println("_"+Thread.currentThread().getName() +  "received msg: " + msg);
                DoMarketTxn(msg);
            }
        }
        return "Market Thread Finished!";
    }

    void DoMarketTxn(String txnFrame){
        String[] parts = txnFrame.split("\\|");
        switch (parts[0]){
            case "MarketFeed":
                transactions.marketFeedFrame(statement);
                break;
            case "TradeResult":
                transactions.tradeResult(statement, parts[1], Double.parseDouble(parts[2]));
                break;
            default:
                System.out.println("Wrong Market Txn type");
                System.out.println(parts[0]);
                break;
        }
    }
}
