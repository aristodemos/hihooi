package hih;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
/**
 * Created by mariosp on 23/1/16.
 */
public class MarketThread extends Thread {

    //List<String> objs = new ArrayList<String>();//init it
    public BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

    String url, pass, user;
    Transactions transactions;
    hihTransactions htransactions;
    Connection conn;
    Statement statement;

    MarketThread(String url ,String user, String pass, Transactions trans){
        this.setName("Market Thread");
        this.transactions = trans;
        this.url = url;
        this.pass = pass;
        this.user = user;
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
