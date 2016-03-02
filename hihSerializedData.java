package hih;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Created by mariosp on 31/1/16.
 */
public class hihSerializedData {

    public static List all_brokers 						= new Vector(10);
    public static List<String> all_sectors 				= new Vector<String>(12);
    public static List<String> all_customers			= new Vector<String>(1000);
    public static List<String> all_symbols 				= new Vector<String>(685);
    //public static List activeSymbols 					= new Vector();
    public static List all_acct_ids 					= new Vector(5000);
    public static Map<String, List<Double>> pricesDM 	= new HashMap<String, List<Double>>();

    private static long nextSeq = 200000000999999L;

    public static void initParams() {
        /*
        all_brokers = dbObject.QUERY("select b_name from broker");
        all_sectors = dbObject.QUERY("select sc_name from sector");
        all_customers = dbObject.QUERY("select c_id from customer");
        all_symbols = dbObject.QUERY("select s_symb from security");
        all_acct_ids = dbObject.QUERY("select ca_id from customer_account");
        */

        all_brokers =   DeseriaizeThings("all_brokers.ser", all_brokers);
        all_sectors =   DeseriaizeThings("all_sectors.ser", all_sectors);
        all_customers = DeseriaizeThings("all_customers.ser", all_customers);
        all_symbols =   DeseriaizeThings("all_symbols.ser", all_symbols);
        all_acct_ids =  DeseriaizeThings("all_acct_ids.ser", all_acct_ids);

        //pricesDM holds avgLow and avgHigh prices of each symbol
        // to be used in the MarketFeed Transaction
        //deserialize
        try (
                InputStream file = new FileInputStream("pricesDM.ser");
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);
        ) {
            //deserialize the Map
            Map<String, List<Double>> rpricesDM = (Map<String,List<Double>>)input.readObject();
            pricesDM = rpricesDM;
        }
        catch (ClassNotFoundException ex){
            ex.printStackTrace();
            System.out.println("Cannot perform input. Class not found");
        }
        catch (IOException ex){
            ex.printStackTrace();
            System.out.println("Cannot perform input");
        }
        //for (String symbol : all_symbols){
        //	System.out.println(pricesDM.get(symbol)  +" low:  " +pricesDM.get(symbol).get(0)+ " high:  "+pricesDM.get(symbol).get(1));
        //}
    }
    static List DeseriaizeThings(String filename, List javaObject){
        try (
                InputStream file = new FileInputStream(filename);
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);
        ) {
            //deserialize the Object
            javaObject = (List)input.readObject();
        }
        catch (ClassNotFoundException ex){
            ex.printStackTrace();
            System.out.println("Cannot perform input. Class not found");
        }
        catch (IOException ex){
            ex.printStackTrace();
            System.out.println("Cannot perform input");
        }
        return javaObject;
    }

    public static String getNextSeqNumber(){
        return Long.toString(nextSeq++);
    }

    public static void setNextSeq(long next){
        nextSeq=next+1;
        System.out.println("SET SEQUENCE NUMBER to "+nextSeq);
    }
}
