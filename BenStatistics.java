package hih;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Created by mariosp on 31/1/16.
 */
public class BenStatistics {

    //constructor
    BenStatistics(){
        for (int i=0; i<groupSize; i++){
            groupDelays.add(new ArrayList<Long>());
        }
    }
    //The pool of transactions - we choose one at random
    public List<String> txnPoolMaster = Arrays.asList("BrokerVolume", "CustomerPosition", "MarketFeed",
            "TradeOrder","TradeResult", "TradeStatus", "SecurityDetail");
    int groupSize = txnPoolMaster.size();
    public long[] txnMix = new long[groupSize];

    public long[] txnDuration 	= new long[groupSize];
    public List<List<Long>>  groupDelays   = new ArrayList<List<Long>>(groupSize);
    private long totalOps = 0;
    private long writeOps = 0;
    //private final Object lock = new Object();

    public void increment(int i) {
        txnMix[i]++;
    }
    public void incOperation(){
        totalOps++;
    }
    public void incWriteOp(){
        writeOps++;
        totalOps++;
        //groupDelays.get(0).add(109L);
    }
    public void insertTime(int i, long timeInterval){
        txnDuration[i]=txnDuration[i]+timeInterval;
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
