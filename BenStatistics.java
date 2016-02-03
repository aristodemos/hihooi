package hih;

import java.util.Arrays;
import java.util.List;

/**
 * Created by mariosp on 31/1/16.
 */
public class BenStatistics {

    //The pool of transactions - we choose one at random
    public static List<String> txnPoolMaster = Arrays.asList("BrokerVolume", "CustomerPosition", "MarketFeed",
            "TradeOrder", "TradeStatus", "SecurityDetail");

    public long[] txnMix 		= new long[txnPoolMaster.size()+1];
    public long[] txnDuration 	= new long[txnPoolMaster.size()+1];
    private long totalOps = 0;
    private long writeOps = 0;
    //private final Object lock = new Object();

    public void increment(int i){
        //synchronized(lock){
        txnMix[i]++;
        //}
    }

    public void incOperation(){
        totalOps++;
    }
    public void incWriteOp(){
        writeOps++;
        totalOps++;
    }

    public void insertTime(int i, long timeInterval){
        //synchronized (lock){
        txnDuration[i]=txnDuration[i]+timeInterval;
        //}
    }

    public long totalTxns(){
        return txnMix[0]+txnMix[1]+txnMix[2]+txnMix[3]+txnMix[4]+txnMix[5]+txnMix[6];
    }
    public long totalOps(){
        return totalOps;
    }
    public long totalWriteOps(){
        return writeOps;
    }
}
