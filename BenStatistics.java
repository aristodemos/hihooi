package hih;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mariosp on 31/1/16.
 */
public class BenStatistics {

    //The pool of transactions - we choose one at random
    public static List<String> txnPoolMaster = Arrays.asList("BrokerVolume", "CustomerPosition", "MarketFeed",
            "TradeOrder", "TradeStatus", "SecurityDetail");

    public static AtomicLong[] txnMix = new AtomicLong[txnPoolMaster.size()+1];

    BenStatistics(){
        for (int i=0; i<txnMix.length; i++){
            txnMix[i] = new AtomicLong(0);
        }
    }


    public long[] txnDuration 	= new long[txnPoolMaster.size()+1];
    private long totalOps = 0;
    private long writeOps = 0;
    //private final Object lock = new Object();

    public void increment(int i){
        //synchronized(lock){
        txnMix[i].incrementAndGet();
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
        //long sum =0;
        //sum = sum + txnMix[0].get();
        return txnMix[0].get()+txnMix[1].get()+txnMix[2].get()+txnMix[3].get()+txnMix[4].get()+txnMix[5].get()
                +txnMix[6].get();
    }
    public long totalOps(){
        return totalOps;
    }
    public long totalWriteOps(){
        return writeOps;
    }
}
