package hih;

import java.sql.Statement;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;
import java.io.ObjectOutputStream;

/**
 * Created by mariosp on 15/10/15.
 */
public class serializeObjects {


    public static List<String> symbols 	= new Vector<String>(685);
    public static List all_brokers 						= new Vector(10);
    public static List<String> all_sectors 				= new Vector<String>(12);
    public static List<String> all_customers			= new Vector<String>(1000);
    public static List<String> all_symbols 				= new Vector<String>(685);
    public static List activeSymbols 					= new Vector();
    private static List all_acct_ids 					= new Vector(5000);

    public static Map<String, List<Double>> s_pricesDM 	= new HashMap<String, List<Double>>();

    static Connection s_conn = null;
    static String url = "jdbc:postgresql://localhost/tpce";
    static String user = "postgres";
    static String pass = "";

    public static void main(String [] args) {

        //arisDemo dbObject = new arisDemo();
        //arisDemo.initParams(dbObject);
        //System.out.println(dbObject.CONNECT());

        String brokers      = "select b_name from broker";
        String sectors      = "select sc_name from sector";
        String customers    = "select c_id from customer";
        String symbols      = "select s_symb from security";
        String acct_ids     = "select ca_id from customer_account";

        //all_brokers = dbObject.QUERY("select b_name from broker");
        //all_sectors
        //all_sectors = dbObject.QUERY("select sc_name from sector");
        //all_customers
        //all_customers = dbObject.QUERY("select c_id from customer");
        //all_symbols
        //all_symbols = dbObject.QUERY("select s_symb from security");
        //account_id
        //all_acct_ids = dbObject.QUERY("select ca_id from customer_account");

        ResultSet rs = null;
        Statement st = null;
        try{

            s_conn = DriverManager.getConnection(url, user, pass);
            st = s_conn.createStatement();

            rs = st.executeQuery(brokers);
            int i=0;
            while (rs.next()){
                all_brokers.add(i,rs.getString("b_name"));
                i++;
            }

            rs = st.executeQuery(sectors);
            i=0;
            while (rs.next()){
                all_sectors.add(i,rs.getString("sc_name"));
                i++;
            }

            rs = st.executeQuery(customers);
            i=0;
            while (rs.next()){
                all_customers.add(i,rs.getString("c_id"));
                i++;
            }

            rs = st.executeQuery(symbols);
            i=0;
            while (rs.next()){
                all_symbols.add(i,rs.getString("s_symb"));
                i++;
            }
            rs = st.executeQuery(acct_ids);
            i=0;
            while (rs.next()){
                all_acct_ids.add(i,rs.getString("ca_id"));
                i++;
            }

            for (String symbol : all_symbols){
                List<Double> values = new ArrayList<Double>();
                String basePriceHigh = String.format("select AVG(dm_high) from daily_market where dm_s_symb = '%s'", symbol);
                rs = st.executeQuery(basePriceHigh);
                double high = 0;
                if (rs.next()) {high = rs.getDouble("avg");}
                String basePriceLow = String.format("select AVG(dm_low) from daily_market where dm_s_symb = '%s'", symbol);
                rs = st.executeQuery(basePriceLow);
                double low = 0;
                if (rs.next()) {low = rs.getDouble("avg");}
                values.add(low);
                values.add(high);
                s_pricesDM.put(symbol, values);
            }
            rs.close(); rs= null;
            st.close(); st = null;

        }catch(Exception e){
            e.printStackTrace();
        }


        SerializeThings("all_brokers.ser", all_brokers);
        SerializeThings("all_sectors.ser", all_sectors);
        SerializeThings("all_customers.ser", all_customers);
        SerializeThings("all_symbols.ser", all_symbols);
        SerializeThings("all_acct_ids.ser", all_acct_ids);

        SerializeThings("pricesDM.ser", s_pricesDM);

        //deserialize
        /*
        try (
                InputStream file = new FileInputStream("pricesDM.ser");
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);
                ) {
            //deserialize the Map
            Map<String, List<Double>> recoveredPricesDM = (Map)input.readObject();
        }
        catch (ClassNotFoundException ex){
            ex.printStackTrace();
            System.out.println("Cannot perform input. Class not found");
        }
        catch (IOException ex){
            ex.printStackTrace();
            System.out.println("Cannot perform input");
        }
        */

    }//end main

     static void SerializeThings(String filename, Object javaObject) {
        System.out.println("Serializing "+filename);
         try (
                OutputStream file = new FileOutputStream(filename);
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutputStream output = new ObjectOutputStream(buffer);
        ) {
            output.writeObject(javaObject);
        } catch (IOException e) {
            System.out.print("Cannot perform output." + e.getMessage());
            e.printStackTrace();
        }
    }

    static void DeseriaizeThings(String filename, List javaObject){
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
    }
}//end class
