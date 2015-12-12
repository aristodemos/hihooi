package hih;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mariosp on 11/12/15.
 */
public class TransactionTicket {
    private final List<Integer> txnsToRun;
    private LinkedList<Integer> txnWindow;
    private Stack<Integer> delayedTransactions;
    private int x;

    public TransactionTicket (List<Integer> ls) {
        txnsToRun = ls;
        x = 0;
        txnWindow = new LinkedList<Integer>(Collections.nCopies(arisDemo.SESSIONS, -1));
        delayedTransactions = new Stack<>();
    }
    private void putInWindow(int c){
        txnWindow.addLast(c);
    }
    public Integer getNextTransaction(int name){
        if (name == 0 && !delayedTransactions.empty()){
            return delayedTransactions.pop();
        }
        if (x == 900){x = 0;}
        txnWindow.removeFirst();

        //εάν το επόμενο στη σειρά τρανσαψτιονς είναι το MarketFeedFrame,
        //λάβε μέτρα ούτως ώστε να αποφύγουμε conflicts
        int a = txnsToRun.get(x+1);
        if (a == 2){
            //αν στο παράθυρο των concurrently running transactions
            //δεν υπάρχει το MarketFeedFrame τότε στείλ'το κανονικά
            if (!writeConflict(txnWindow, 2)){
                putInWindow(a);
                x++;
                return a;
            }else{ //We have a conflict. Next Trans is MF and a MF trxn is running already...
                //put it in the Queue
                delayedTransactions.push(a);
                x++;
                //Recursion to the Rescue
                return getNextTransaction(name);
                // SO now we have to deal with all the transactions in the Queue
            }
        }
        else { //not a MarketFeed Frame
            // if there isn't a window conflict and there exists something in the queue, send from the queue
            /*if(!delayedTransactions.empty() && !writeConflict(txnWindow, 2)){
                //sending from queue
                putInWindow(2);
                return delayedTransactions.pop();
            }*/
            putInWindow(a);
            x++;
            return a;
        }
    }

    private boolean writeConflict(List<Integer> l, int x){
        return l.contains(x);
    }
}
