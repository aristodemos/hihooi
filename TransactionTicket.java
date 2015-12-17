package hih;

//import sun.jvm.hotspot.utilities.*;
//import sun.jvm.hotspot.utilities.Hashtable;

import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mariosp on 11/12/15.
 */
public class TransactionTicket {
    private static List<Integer> txnsToRun;
    private static List<Integer> txnWindow;
    private Stack<Integer> delayedTransactions;
    private int x;
    private int winLength;

    public TransactionTicket (List<Integer> ls, int sessions) {
        txnsToRun = ls;
        x = 0;
        winLength = sessions;
        txnWindow = new ArrayList<>(Collections.nCopies(winLength, -1));
        delayedTransactions = new Stack<>();
    }

    public Integer getNextTransaction(int name){
        //System.out.println("TxnWind: " + txnWindow + "\t thread: " + name);
        try{
            return txnsToRun.get(x++);
        }catch (IndexOutOfBoundsException iobe){
            x=0;
            return txnsToRun.get(x);
        }

        /*
        if (!delayedTransactions.empty() && !txnWindow.contains(2)){
            int a = delayedTransactions.pop();
            txnWindow.set(name%winLength, a);
            return a;
        }
        txnWindow.set(name%winLength, -1);

        //εάν το επόμενο στη σειρά τρανσαψτιον είναι το MarketFeedFrame,
        //λάβε μέτρα ούτως ώστε να αποφύγουμε conflicts
        int a;
        try{a = txnsToRun.get(x+1);}
        catch(IndexOutOfBoundsException iobe){
            x=0;
            a=txnsToRun.get(0);
        }
        if (a == 2){
            //We have a conflict. Next Trans is MF and a MF trxn is running already...
            if (txnWindow.contains(2)){
                //put it in the Queue
                delayedTransactions.push(a);  // SO now we have to deal with all the transactions in the Queue
                x++;
                //Recursion to the Rescue
                return getNextTransaction(name);

            }else{
                //αν στο παράθυρο των concurrently running transactions
                //δεν υπάρχει το MarketFeedFrame τότε στείλ'το κανονικά
                txnWindow.set(name%winLength, a);
                x++;
                return a;
            }
        }
        else { //not a MarketFeed Frame
            // if there isn't a window conflict and there exists something in the queue, send from the queue
            /*if(!delayedTransactions.empty() && !writeConflict(txnWindow, 2)){
                //sending from queue
                putInWindow(2);
                return delayedTransactions.pop();
            }
            txnWindow.set(name%winLength, a);
            x++;
            return a;

        }
        */
    }

    public List<Integer> getNexTransactionSet(int name, int length){
        List<Integer> toReturn = new ArrayList<Integer>(length);
        for(int i=0; i<length;i++){
            toReturn.add(getNextTransaction(name));
        }
        return toReturn;
    }
}
