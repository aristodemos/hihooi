package hih;

import java.io.*;
import java.util.*;
import java.io.ObjectOutputStream;

/**
 * Created by mariosp on 15/10/15.
 */
public class serializeObjects {


    public static List<String> symbols 	= new Vector<String>(685);
    public static Map<String, List<Double>> s_pricesDM 	= new HashMap<String, List<Double>>();

    public static void main(String [] args) {

        arisDemo dbObject = new arisDemo();
        //arisDemo.initParams(dbObject);
        dbObject.CONNECT();

        symbols = dbObject.QUERY("select s_symb from security");

        for (String symbol : symbols){
            List<Double> values = new ArrayList<Double>();
            String basePriceHigh = String.format("select AVG(dm_high) from daily_market where dm_s_symb = '%s'",
                    symbol);
            double high = Double.parseDouble(dbObject.QUERY2MAP(basePriceHigh).get("avg").toString());
            String basePriceLow = String.format("select AVG(dm_low) from daily_market where dm_s_symb = '%s'",
                    symbol);
            double low = Double.parseDouble(dbObject.QUERY2MAP(basePriceLow).get("avg").toString());
            values.add(low);
            values.add(high);
            s_pricesDM.put(symbol, values);
        }

        try (
                OutputStream file   = new FileOutputStream("pricesDM.ser");
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutputStream output = new ObjectOutputStream(buffer);
        ){
            output.writeObject(s_pricesDM);
        }
        catch (IOException e){
            System.out.print("Cannot perform output." + e.getMessage());
            e.printStackTrace();
        }

        dbObject.DISCONNECT();
        //deserialize
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

    }//end main
}//end class
