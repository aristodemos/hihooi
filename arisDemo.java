package hih;

//import com.sun.tools.javac.code.Attribute;

import java.io.FileNotFoundException;
import java.io.IOException;
//import java.lang.reflect.Array;
import java.io.UnsupportedEncodingException;
import java.sql.*;
//import java.text.DateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.Arrays;
//import java.util.Calendar;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicLongArray;

public class arisDemo {


	static final class Statistics {
		private long[] txnMix = new long[2*(txnPoolMaster.size()+1)];
		//private final Object lock = new Object();

		public void increment(int i){
			//synchronized(lock){
				txnMix[i]++;
			//}
		}

		public void insertTime(int i, long timeInterval){
			//synchronized (lock){
				txnMix[i]=txnMix[i]+timeInterval;
			//}
		}

		public long txnsPerSec(){
			 return txnMix[0]+txnMix[1]+txnMix[2]+txnMix[3]+txnMix[4]+txnMix[5];
		}
	}

	private String LISTENER="52.24.138.123";
	//private String LISTENER="172.30.0.206";
	private HiHListenerClient hih = new HiHListenerClient();

	public String CONNECT()
	{
		Properties p = new Properties();
		p.setProperty("server",this.LISTENER);
		p.setProperty("port","7777");
		p.setProperty("username","user01");
		p.setProperty("password","12345678");
		p.setProperty("identifier","client01");
		p.setProperty("service_name","TESTSRV");
		return hih.connect(p);   /// Retun SESSION-ID e.g  21703567-1ed7-4f59-aeac-39686ea9c2b1
	}
	public String setConsistency(String setCMD) {
		/*
		* SET COMMANDS:
      		set consistency level 1             --Hihooi
      		set consistency level 2             --Eventually
		*/
		return hih.set(setCMD);
	}

	public String DISCONNECT()
	{
		return hih.disconnect();  //Returns "Disconnect Successfuly"
	}

	//GLOBALS used to generate the txns for the benchmark
	public static List all_brokers 			= new Vector(10);
	public static List<String> all_sectors 	= new Vector<String>(12);
	public static List<String> all_customers	= new Vector<String>(1000);
	public static List<String> all_symbols 	= new Vector<String>(685);
	private static List activeSymbols 			= new Vector();
	private static List all_acct_ids 			= new Vector(5000);

	//public static Random globalRand = ThreadLocalRandom.current();
	// added global random to improve the txnMix;   8 matches
	//public static int trxnsPerSession   = 10;
    public static int       SESSIONS        = 20;
	public static int       TIMETORUN       = 40;
	public static String    MIXSELECTOR   	= "a"; //default: all transactions
    private static boolean  DEBUG           = false;
    private static String   LAST_T_ID       = "200000000290880";

	//The writer for the Log
	public static PrintWriter logWriter = null;

	//The pool of transactions - we choose one at random
	public static List<String> txnPoolMaster = Arrays.asList("BrokerVolume", "CustomerPosition", "MarketFeed",
			"TradeOrder", "TradeStatus");

	//Atomic Array of Long to save the total latency(cummulative) and total number of each transaction
	//private static  long[] txnMix = new long[2*(txnPoolMaster.size()+1)];

	public String EXEC_QUERY(String SQL) {
		//if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		List<Map<String, Object>> rows = hih.executeQuery(SQL);
		String output ="";
		for( int i = rows.size() -1; i >= 0 ; i --) {
			Map<String,Object> entry = rows.get(i);
			for (String key : entry.keySet()) {
				output = ""+entry.get(key);
			}
		}
		return output;
	}

	public List QUERY(String SQL) {
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		List<Map<String, Object>> rows = hih.executeQuery(SQL);
		String output ="";
		List resultOut = new Vector();
		for( int i = rows.size() -1; i >= 0 ; i --)
		{
			Map<String,Object> entry = rows.get(i);
			for (String key : entry.keySet())
			{
				output += entry.get(key)+" ";
				resultOut.add(entry.get(key));
			}
		}
		return resultOut;
	}

	public String QUERY2STR(String SQL){
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		List<Map<String, Object>> rows = hih.executeQuery(SQL);
		String output ="";
		for( int i = rows.size() -1; i >= 0 ; i --)
		{
			Map<String,Object> entry = rows.get(i);
			for (String key : entry.keySet())
			{
				output += entry.get(key);
			}
		}
		return output;
	}

	public Map QUERY2MAP(String SQL){
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		List<Map<String, Object>> rows = hih.executeQuery(SQL);
		Map<String, Object> results = new HashMap<>();
		for( int i = rows.size() -1; i >= 0 ; i --) {
			Map<String,Object> entry = rows.get(i);
			for (String key : entry.keySet()){
				results.put(key, entry.get(key));
			}
		}
		return results;
	}

	public List QUERY2LST(String SQL){
        //if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		List<Map<String, Object>> rows = hih.executeQuery(SQL);
		String output ="";
		for( int i = rows.size() -1; i >= 0 ; i --)
		{
			Map<String,Object> entry = rows.get(i);
			for (String key : entry.keySet())
			{
				output += entry.get(key);
			}
		}
		return rows;
	}

	//Data Manipulation Language: INSERT, DELETE, UPDATE;
	public String DML(String SQL) {
        //if (DEBUG){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		return hih.executeUpdate(SQL);
	}

	public String START_TX(String description) {
        if (DEBUG){logWriter.printf("START_TX : %d; %s \n", System.currentTimeMillis(), description);}
		return hih.startTransaction();
	}
	//TRANSACTION CONTROL LANGUAGE: COMMIT, ROLLBACK;
	public String TCL(String tcl_cmd, String description) {
        if (DEBUG){logWriter.printf("%s : %d;  %s \n", tcl_cmd, System.currentTimeMillis(), description);}
		if (tcl_cmd.equalsIgnoreCase("commit")) {
			return hih.commitTransaction();
		}
		else {
			return hih.rollbackTransaction();
		}
	}

	public arisDemo() {}


	public static void main(String [] args) throws IOException {
		if (args.length > 0) {
			for (int i=0; i< args.length; i++) {
				if (args[i].equalsIgnoreCase("-sessions") ||args[i].equalsIgnoreCase("-s")) {
					SESSIONS = Integer.parseInt(args[i + 1]);
				}
				/*
				*if (args[i].equalsIgnoreCase("-txns") || args[i].equalsIgnoreCase("-t")) {
				*	trxnsPerSession= Integer.parseInt(args[i + 1]);
				*}
				*/
				if (args[i].equalsIgnoreCase("-mix") || args[i].equalsIgnoreCase("-m")) {
					MIXSELECTOR = args[i + 1];
				}
			}
		}

		//Create transaction Log File
		//Remember to close it
		try{
			logWriter = new PrintWriter("tps_mix_"+MIXSELECTOR+".txt", "UTF-8");

		}catch (FileNotFoundException |UnsupportedEncodingException err){
			System.out.println("FileNOTfound" + err.getMessage());
		}

		Locale.setDefault(Locale.US);
		System.out.println("*************** Initializing the Test Run ******************");
		System.out.println("************************************************************");
        System.out.println("Using Mix "+MIXSELECTOR);
		//init Statistics
		final Statistics stats = new Statistics();


		arisDemo d = new arisDemo();
		System.out.println(d.CONNECT());
		System.out.println("Connection Established");

		//System.out.println("Cleaning Up database");
		//preInitRun(d);

		//Gather necessary data from the initialized database;
		System.out.println("Initializing Parameters");
		initParams(d);
		System.out.println("Disconnecting after initialization...");
		System.out.println(d.DISCONNECT());

		System.out.println("Starting Sessions");
		System.out.println("Number of threads (sessions):  "+ SESSIONS);
		//System.out.println("Txns per Session :  " + trxnsPerSession);

		logWriter.printf("Number of threads (sessions):  " + SESSIONS + "\r\n");
		//logWriter.printf("Txns per Session :  " + trxnsPerSession+"\r\n");

		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		exec.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				// do stuff
				logWriter.print(stats.txnsPerSec() + "\r\n");
			}
		}, 0, 5, TimeUnit.SECONDS);


			ExecutorService pool = Executors.newFixedThreadPool(SESSIONS);
			List<Future<String>> list = new ArrayList<Future<String>>();

            Collection<SimTest> collection = new ArrayList<>();
            for(int i=0; i< SESSIONS; i++){
                SimTest task = new SimTest(stats);
                collection.add(task);
            }
            long startTime = System.currentTimeMillis(); //fetch starting time
        try {
            List<Future<String>> listF = pool.invokeAll(collection, TIMETORUN, TimeUnit.MINUTES);
            for(Future<String> fut: listF){
                System.out.println("Time for Session "+fut.get());
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally {
            long endTime = System.currentTimeMillis(); //end time
            long duration = endTime - startTime;
            pool.shutdown();
            System.out.println(" ALL SESSIONS COMPLETED IN " +duration+" msec \n");
            System.out.println(" which equals " + (duration / (1000 * 60.0)) + " minutes \n");
        }
            //TODO: check if the above piece of code works
            /*
			Callable<String> callable = new  SimTest(stats);
			for(int i=0; i< SESSIONS; i++)
			{
				//submit Callable tasks to be executed by thread pool
				Future<String> future = pool.submit(callable);
				//add Future to the list, we can get return value using Future
				list.add(future);
			}

			for(Future<String> fut : listF) {
				try {
					System.out.println("Time for Session "+fut.get());
				}
				catch(Exception e) {
					e.printStackTrace();
				}
			}

			pool.shutdownNow();
			while (!pool.isTerminated()) {
				//this can throw InterruptedException, you'll need to decide how to deal with that.
				pool.awaitTermination(1,TimeUnit.MILLISECONDS);
			}
			long endTime = System.currentTimeMillis(); //end time
			long duration = endTime - startTime;
            */
			//Close the transaction Log File
			logWriter.close();
			exec.shutdownNow();

			//PRINT STATS
			System.out.println("*********************Test Run statistics********************");
			System.out.println("************************************************************");
			System.out.println("Txn Mix:");
			System.out.println("BrokerVolume Txn was run: \t\t"+stats.txnMix[0]  + " times;");
			System.out.println("CustomerPosition Txn was run: \t"+stats.txnMix[1] + " times;");
			System.out.println("MaretFeed Txn was run: \t\t\t"+stats.txnMix[2] + " times;");
			System.out.println("TradeOrder Txn was run: \t\t"+stats.txnMix[3] + " times;");
			System.out.println("TradeResult Txn was run: \t\t"+stats.txnMix[4] + " times;");
			System.out.println("TradeStatus Txn was run: \t\t"+stats.txnMix[5] + " times;");

			long totalTxns = stats.txnMix[0]+stats.txnMix[1]+stats.txnMix[2]+stats.txnMix[3]+stats
					.txnMix[4]+stats.txnMix[5];
			System.out.println("Total Number of Txns run: \t\t"+totalTxns);

			System.out.println("\n\n****************** Txn Duration (in msec) ******************");
			System.out.println("************************************************************");
			System.out.println("Broker Volume avg time\t\t: " + ((double) stats.txnMix[6])/stats.txnMix[0]);
			System.out.println("Customer Position avg time\t: " + ((double) stats.txnMix[7])/stats.txnMix[1]);
			System.out.println("Market Feed avg time\t\t: " + ((double) stats.txnMix[8])/stats.txnMix[2]);
			System.out.println("Trade Order avg time\t\t: " + ((double) stats.txnMix[9])/stats.txnMix[3]);
			System.out.println("Trade Result avg rime\t\t: " + ((double) stats.txnMix[10])/stats.txnMix[4]);
			System.out.println("Trade Status avg rime\t\t: " + ((double) stats.txnMix[11])/stats.txnMix[5]);


	}
	// TODO: THIS METHOD MUST RUN ON ALL(!) DATABASE INSTANCES
	// PRIMARY AND ALL EXTENSION DBs
 	private static void preInitRun(arisDemo dbConn){
		//RUNTRADE CLEANUP TRANSACTION TO BRING THE DATABASE TO A KNOWN STATE
		dbConn.EXEC_QUERY(String.format("SELECT * FROM TradeCleanupFrame1('CNCL', 'SBMT', 'PNDG', %s)", LAST_T_ID));
		System.out.println("Database Cleaned...");
	}

	public static void initParams(arisDemo dbObject) {
		//all_brokers
		all_brokers = dbObject.QUERY("select b_name from broker");
		//all_sectors
		all_sectors = dbObject.QUERY("select sc_name from sector");
		//all_customers
		all_customers = dbObject.QUERY("select c_id from customer");
		//all_symbols
		all_symbols = dbObject.QUERY("select s_symb from security");
		//account_id
		all_acct_ids = dbObject.QUERY("select ca_id from customer_account");
	}

	//TODO: transaction Mix Variations
	// a: only reads	: 100 - 00
	// b: 5% writes		: 095 - 05
	// c: 10% writes	: 090 - 10
	// d: 30% writes	: 070 - 30
	public static List workloadMix(String selector) {
		List<String> pool = new Vector<String>();
		switch(selector) {
			case "a":
				for (int i=0; i<50;i++){
					pool.add("BrokerVolume");
			}
				for (int i=50; i<80;i++){
					pool.add("TradeStatus");
				}
				for (int i=80;i<100;i++){
					pool.add("CustomerPosition");
				}
				return randomSample(pool, pool.size());
			case "b":
				for (int i=0; i<25;i++){
					pool.add("BrokerVolume");
				}
				for (int i=25; i<50;i++){
					pool.add("CustomerPosition");
				}
				for (int i=50;i<95;i++){
					pool.add("TradeStatus");
				}
				for (int i=95;i<96;i++){
					pool.add("MarketFeed");
				}
				for (int i=96;i<98;i++){
					pool.add("TradeOrder");
				}
				return randomSample(pool, pool.size());
			case "c":
				for (int i=0; i<20;i++){
					pool.add("BrokerVolume");
				}
				for (int i=20; i<45;i++){
					pool.add("CustomerPosition");
				}
				for (int i=45;i<90;i++){
					pool.add("TradeStatus");
				}
				for (int i=90;i<94;i++){
					pool.add("MarketFeed");
				}
				for (int i=94;i<97;i++){
					pool.add("TradeOrder");
				}
				return randomSample(pool, pool.size());
			//case 'd' is the default
			default:
				for (int i=0; i<15;i++){
					pool.add("BrokerVolume");
				}
				for (int i=15; i<30;i++){
					pool.add("CustomerPosition");
				}
				for (int i=30;i<70;i++){
					pool.add("TradeStatus");
				}
				for (int i=70;i<80;i++){
					pool.add("MarketFeed");
				}
				for (int i=80;i<90;i++){
					pool.add("TradeOrder");
				}
				return randomSample(pool, pool.size());

		}
	}

	/*
    private static List shuffleTxns(){
        List<String> pool = new Vector<String>();
        int interval = trxnsPerSession/(txnPoolMaster.size()+1);
        for (int j=0; j<txnPoolMaster.size(); j++) {
            for (int i=0; i<interval; i++) {
                pool.add(txnPoolMaster.get(j));
            }
        }
        int diff = trxnsPerSession - (txnPoolMaster.size()+1)*interval ;
        if (diff > 0){
            for (int i=0; i<diff; i++){
                pool.add("TradeStatus");
            }
        }
        return randomSample(pool, pool.size());
    }

    public static double getAverage(Vector<Long> list) {
		double sum = 0.0;
		for (int i=0; i<list.size(); i++){
			sum += list.get(i);
		}
		return sum/list.size();
	}

    */

	private static void generateTxn(arisDemo d, String txnFrame, Statistics stats) {
		switch (txnFrame)
		{
			case "BrokerVolume":
				//System.out.print("Executing Broker Volume from: ");
				//System.out.println(Thread.currentThread());
				brokerVolumeFrame(d, stats);
				stats.increment(0);
				//txnMix[0]++;
				break;
			case "CustomerPosition":
				//System.out.print("Executing Customer Position from: ");
				//System.out.println(Thread.currentThread());
				customerPositionFrame(d, stats);
				stats.increment(1);
				break;
			case "MarketFeed":
				//System.out.print("Executing Market Feed from: ");
				//System.out.println(Thread.currentThread());
				marketFeedFrame(d, stats);
				stats.increment(2);
				break;
			case "TradeOrder":
				//System.out.print("Executing Trade Order followed by Trade Result from: ");
				//System.out.println(Thread.currentThread());
				tradeOrder(d, stats);
				stats.increment(3);
				break;
			case "TradeStatus":
				//System.out.print("Executing Trade Status from: ");
				//System.out.println(Thread.currentThread());
				tradeStatus(d, stats);
				stats.increment(5);
				break;
			//default: "";
			//	break;
		}
	}
	//random sampling - a bounded version of Knuth's algorithm
	//taken from https://eyalsch.wordpress.com/2010/04/01/random-sample/
	public static <T> List<T> randomSample(List<T> items, int m){
		for(int i=0;i<m;i++){
			int pos = i + ThreadLocalRandom.current().nextInt(items.size() - i);
			T tmp = items.get(pos);
			items.set(pos, items.get(i));
			items.set(i, tmp);
		}
		return items.subList(0, m);
	}

	private static void brokerVolumeFrame(arisDemo dbObject, Statistics s) {
		int number_of_brokers = ThreadLocalRandom.current().nextInt(1, all_brokers.size());
		List active_brokers = randomSample(all_brokers, number_of_brokers);
		String sector_name = all_sectors.get(ThreadLocalRandom.current().nextInt(0, all_sectors.size()));
		Long t;
 		//convert active_brokers list to string seperated by commas
		String activeBrokersStr = org.apache.commons.lang3.StringUtils.join(active_brokers, ',');
		//for (int i=0; i<active_brokers.size(); i++) {
			String query = String.format("SELECT b_name, SUM(tr_qty * tr_bid_price) AS vol " +
					"FROM trade_request, sector, industry, company, broker, security " +
					"WHERE tr_b_id = b_id " +
					"AND tr_s_symb = s_symb " +
					"AND s_co_id = co_id " +
					"AND co_in_id = in_id " +
					"AND sc_id = in_sc_id " +
					"AND b_name = ANY ('{%s}')" +			//'%s' " +//"AND b_name =
					"AND sc_name = '%s' " +
					"GROUP BY b_name " +
					"ORDER BY 2 DESC", activeBrokersStr, sector_name);  //actoive.brokers.get(i)
			//System.out.println(query);
			t = System.currentTimeMillis();
			dbObject.QUERY(query);
			//
			s.insertTime(6, System.currentTimeMillis() - t);
			//s.txnMix[6] = s.txnMix[6] + System.currentTimeMillis() - t;
		//}
	}

	private static void customerPositionFrame(arisDemo dbObject, Statistics s) {
		Long t = System.currentTimeMillis();
		//Customer Position Frame 1 of 2
		String cust_id = all_customers.get(ThreadLocalRandom.current().nextInt(0, all_customers.size()));
		String query1 = String.format(
				"SELECT CA_ID, CA_BAL, sum(HS_QTY * LT_PRICE) as soma " +
						"FROM CUSTOMER_ACCOUNT left outer join " +
						"HOLDING_SUMMARY on HS_CA_ID = CA_ID, LAST_TRADE " +
						"WHERE CA_C_ID = '%s' " +
						"GROUP BY CA_ID, CA_BAL " +
						"ORDER BY 3 asc " +
						"LIMIT 10", cust_id);
		//System.out.println(query1);

		dbObject.QUERY(query1);

		//Customer Position Frame 2 of 2

		//String c_ad_id = dbObject.QUERY2STR(String.format("select c_ad_id from customer where c_id = '%s'", cust_id));
		//System.out.printf("select c_ad_id from customer where c_id = '%s' \n", cust_id);
        String c_ad_id = "4300000189";
        String q = String.format("select c_ad_id from customer where c_id = '%s'", cust_id);
        try {
            c_ad_id = dbObject.QUERY2STR(q);
        }catch (NullPointerException e){
            System.out.println("Null Pointer Exception in CustomerPositionFrame1");
            System.out.println(dbObject.QUERY(q));
            System.out.println(dbObject.QUERY2STR(q));
            System.out.println(dbObject.QUERY2MAP(q));
            System.out.println(dbObject.QUERY2LST(q));
            //System.out.println(String.format("select c_ad_id from customer where c_id = '%s'", cust_id));
            //return;
        }
		String query2 = String.format(
				"SELECT T_ID, T_S_SYMB, T_QTY, ST_NAME, TH_DTS " +
				"FROM (SELECT T_ID as ID " +
				"FROM TRADE " +
				"WHERE T_CA_ID = '%s' " +
				"ORDER BY T_DTS desc LIMIT 10) as T, " +
				"TRADE, TRADE_HISTORY, STATUS_TYPE " +
				"WHERE T_ID = ID " +
				"AND TH_T_ID = T_ID " +
				"AND ST_ID = TH_ST_ID " +
				"ORDER BY TH_DTS desc " +
				"LIMIT 30", c_ad_id);
		//System.out.println(query2);
		dbObject.QUERY(query2);
		s.insertTime(7, System.currentTimeMillis() - t);
		//s.txnMix[7] = s.txnMix[7] + System.currentTimeMillis() - t;
	}

	private static void marketFeedFrame(arisDemo dbObject, Statistics s) {
		Long t = System.currentTimeMillis();

		activeSymbols = dbObject.QUERY("select TR_S_SYMB from TRADE_REQUEST");
		//int numberOfSymbols = ThreadLocalRandom.current().nextInt(0, activeSymbols.size());
		//the previous line is commented out because the spec states that the number of symbols must be 20;
		int numberOfSymbols = 20;
		if (activeSymbols.size() < 20) {numberOfSymbols = activeSymbols.size();}
		if (numberOfSymbols == 0) {return;}
		List activeSymbolsSet = randomSample(activeSymbols, numberOfSymbols);

		dbObject.START_TX("marketFeed");
		//price quote[]
		ArrayList<Double> priceQuote = new ArrayList<Double>(numberOfSymbols);
		for (int i=0; i<numberOfSymbols; i++){
			String basePriceHigh = String.format("select AVG(dm_high) from daily_market where dm_s_symb = '%s'",
					activeSymbolsSet.get(i));
			//double high = Double.parseDouble(dbObject.QUERY2STR(basePriceHigh));
			double high = Double.parseDouble(dbObject.QUERY2MAP(basePriceHigh).get("avg").toString());
			String basePriceLow = String.format("select AVG(dm_low) from daily_market where dm_s_symb = '%s'",
					activeSymbolsSet.get(i));
			double low = Double.parseDouble(dbObject.QUERY2MAP(basePriceLow).get("avg").toString());
			priceQuote.add(i, ThreadLocalRandom.current().nextDouble(low, high));
		}

		//trade quantity[]
		ArrayList<String> tradeQuantity= new ArrayList<String>(numberOfSymbols);
		for (int i=0; i<numberOfSymbols; i++){
			String tradeQtQuery = String.format("select tr_qty from trade_request where tr_s_symb = '%s'",
					activeSymbolsSet.get(i));
			tradeQuantity.add(dbObject.QUERY2MAP(tradeQtQuery).get("tr_qty").toString());
		}
		for (int i=0; i<numberOfSymbols; i++) {
			String query1 = String.format(
					"UPDATE LAST_TRADE " +
							"SET LT_PRICE = %f, " +
							"LT_VOL = LT_VOL + %s, " +
							"LT_DTS = now() " +
							"WHERE LT_S_SYMB = '%s'", priceQuote.get(i), tradeQuantity.get(i), activeSymbolsSet.get(i));
			//store trade_id in request_list
			dbObject.DML(query1);
			String query2 = String.format(
					"SELECT TR_T_ID "+
					"FROM TRADE_REQUEST " +
					"WHERE TR_S_SYMB = '%s' and " +
					"((TR_TT_ID = 'TSL' and TR_BID_PRICE >= %.2f) or " +
					"(TR_TT_ID = 'TLS' and TR_BID_PRICE <= %.2f) or " +
					"(TR_TT_ID = 'TLB' and TR_BID_PRICE >= %.2f))",activeSymbolsSet.get(i),priceQuote.get(i),
					priceQuote.get(i),priceQuote.get(i));
			List<Map<String, Object>> request_list = dbObject.QUERY2LST(query2);
			//loop over request_list
			for (int j=0; j<request_list.size();j++) {
				String query3 =String.format("UPDATE TRADE " +
						"SET T_DTS = now(), " +
						"T_ST_ID = 'SBMT' " +
						"WHERE T_ID = %s", request_list.get(j).get("tr_t_id"));
				String query4 = String.format("DELETE FROM TRADE_REQUEST " +
						"WHERE TR_T_ID = %s", request_list.get(j).get("tr_t_id"));

				String query5 = String.format( "INSERT INTO TRADE_HISTORY " +
						" VALUES (%s, now(), 'SBMT')", request_list.get(j).get("tr_t_id"));

				dbObject.DML(query3);
				dbObject.DML(query4);
				dbObject.DML(query5);
			}
		}
		dbObject.TCL("commit", "marketfeed");
		s.insertTime(8, System.currentTimeMillis() - t);
		//s.txnMix[8] = s.txnMix[8] + System.currentTimeMillis() - t;
	}

	//Important:
	//After a successfully Committed market order, the EGenTxnHarness sends the order for the trade to the appropriate MEE.
	//In other wors the trade Result txn is called !!!
	private static void tradeOrder(arisDemo dbObject, Statistics s) {
		Long t = System.currentTimeMillis();
		//Frame Inputs: account_id, symbol of the security (stock), trade_quantity
		//bools: type_is_market, type_is_sell, use LIFO on FILO traversal
		//
		//TEST SET
		//String acct_id = "43000000804";
		//String symbol = "TWTR";
		//boolean type_is_sell = true;
		//boolean is_lifo = true;
		String acct_id =  all_acct_ids.get(ThreadLocalRandom.current().nextInt(0, all_acct_ids.size())).toString();

		String symbol = all_symbols.get(ThreadLocalRandom.current().nextInt(0, all_symbols.size()));

		int trade_qty = ThreadLocalRandom.current().nextInt(100, 800);
		boolean type_is_market 	= ThreadLocalRandom.current().nextBoolean();
		boolean type_is_sell	= ThreadLocalRandom.current().nextBoolean();

		String t_tt_id = "";
		if (type_is_market){
			if (type_is_sell) {t_tt_id = "TMS";}
			else {t_tt_id = "TMB";}
		}
		else {
			if (type_is_sell) {t_tt_id = "TLS";}
			else {t_tt_id = "TLB";}
		}
		boolean is_lifo	= ThreadLocalRandom.current().nextBoolean();
		boolean t_is_cash	= ThreadLocalRandom.current().nextBoolean();

		//START TXN
		dbObject.START_TX("tradeOrder");

		// Get account, customer, and broker information into a Map
		String  sqlTOF1_1 = String.format(
				"SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st " +
						"FROM customer_account " +
						"WHERE ca_id = %s", acct_id);
		Map output1 = dbObject.QUERY2MAP(sqlTOF1_1);
		//System.out.println(sqlTOF1_1);
		String exec_name = output1.get("ca_name").toString();

		//Broker ID
		String broker_id = output1.get("ca_b_id").toString();

		String  sqlTOF1_2 = String.format(
                "SELECT c_f_name, c_l_name, c_tier, c_tax_id " +
                        "FROM customer " +
                        "WHERE c_id = %s", output1.get("ca_c_id"));
		Map output2 = dbObject.QUERY2MAP(sqlTOF1_2);

		String  sqlTOF1_3 = String.format(
				"SELECT b_name " +
						"FROM Broker  " +
						"WHERE b_id = %s", broker_id);
		Map output3 = dbObject.QUERY2MAP(sqlTOF1_3);

		String  sqlTOF2_1 = String.format(
                "SELECT ap_acl  " +
                        "FROM account_permission " +
                        "WHERE ap_ca_id = %s " +
                        "  AND ap_f_name = '%s' " +
                        "  AND ap_l_name = '%s' " +
                        "  AND ap_tax_id = '%s'", acct_id, output2.get("c_f_name"), output2.get("c_l_name"), output2
                        .get("c_tax_id"));
		Map output4 = dbObject.QUERY2MAP(sqlTOF2_1);
		//TODO: check this rollback!
        if (output1.isEmpty()){
			dbObject.TCL("rollback", "inTradeOrder");
			return;
		}

		//trade using company name. Otherwise trade using symbol
		/*String  SQLTOF3_1a = String.format(
				"SELECT co_id " +
						"FROM company " +
						"WHERE co_name = '%s'",);

		String  SQLTOF3_2a = String.format(
				"SELECT s_ex_id, s_name, s_symb " +
						"FROM security " +
						"WHERE s_co_id = %s " +
						"  AND s_issue = '%s'",);*/

		String  sqlTOF3_1b = String.format(
                "SELECT s_co_id, s_ex_id, s_name " +
                        "FROM security " +
                        "WHERE s_symb = '%s' ", symbol);
		//System.out.println(sqlTOF3_1b);
		Map output5 = dbObject.QUERY2MAP(sqlTOF3_1b);

		String  sqlTOF3_2b = String.format(
				"SELECT co_name " +
						"FROM company " +
						"WHERE co_id = %s", output5.get("s_co_id"));
		Map co_name = dbObject.QUERY2MAP(sqlTOF3_2b);

		String  sqlTOF3_3 = String.format(
				"SELECT lt_price " +
						"FROM last_trade " +
						"WHERE lt_s_symb = '%s'", symbol);
		Map price = dbObject.QUERY2MAP(sqlTOF3_3);
		String getPrice = price.get("lt_price").toString();
		double trade_price = Double.parseDouble(getPrice);



		/*String  SQLTOF3_4 = String.format(
				"SELECT tt_is_mrkt, tt_is_sell " +
						"FROM trade_type " +
						"WHERE tt_id = '%s'",);*/


		// Local frame variables used when estimating impact of this trade on // any current holdings of the same security.
		double hold_price;
		// Initialize variables
		double buy_value = 0.0, sell_value = 0.0;
		int hold_qty = 0, needed_qty = trade_qty;

		String  sqlTOF3_5 = String.format(
                "SELECT hs_qty " +
                        "FROM holding_summary " +
                        "WHERE hs_ca_id = %s " +
                        "  AND hs_s_symb = '%s'", acct_id, symbol);
		Map hs_qtyMap = dbObject.QUERY2MAP(sqlTOF3_5);
		if (!hs_qtyMap.isEmpty()) {hold_qty = Integer.valueOf((String) hs_qtyMap.get("hs_qty"));}


		String  sqlTOF3_6a = String.format(
				"SELECT h_qty, h_price " +
						"FROM holding " +
						"WHERE h_ca_id = %s " +
						"  AND h_s_symb = '%s' " +
						"ORDER BY h_dts DESC", acct_id, symbol);

		String  sqlTOF3_6b = String.format(
				"SELECT h_qty, h_price " +
						"FROM holding " +
						"WHERE h_ca_id = %s " +
						"  AND h_s_symb = '%s' " +
						"ORDER BY h_dts ASC", acct_id, symbol);
		List holdList =new ArrayList<Map<String, Object>>();
		if (type_is_sell) {
		// This is a sell transaction, so estimate the impact to any currently held
		// long postions in the security.
		 	if (hold_qty > 0 ){
				if (is_lifo){
					// Estimates will be based on closing most recently acquired holdings
					// Could return 0, 1 or many rows
					holdList = dbObject.QUERY2LST(sqlTOF3_6a);
				}
				else {
					// Estimates will be based on closing oldest holdings
					// Could return 0, 1 or many rows
					holdList = dbObject.QUERY2LST(sqlTOF3_6b);
				}
				// Estimate, based on the requested price, any profit that may be realized
				// by selling current holdings for this security. The customer may have
				// multiple holdings at different prices for this security (representing
				// multiple purchases different times).
				if (!holdList.isEmpty()){
					for(int i=0; i<holdList.size(); i++){
						//System.out.println(holdList.get(i));
						Map entry = (Map) holdList.get(i);
						if ((int) entry.get("h_qty") > needed_qty){
							buy_value += needed_qty * (double) entry.get("h_price");
							sell_value += needed_qty * trade_price;
							needed_qty = 0;
							break;
						}
						else {
							buy_value += needed_qty * (double) entry.get("h_price");
							sell_value += needed_qty * trade_price;
							needed_qty = needed_qty - (int) entry.get("h_qty");
						}
					}
		 		}
				// NOTE: If needed_qty is still greater than 0 at this point, then the
				// customer would be liquidating all current holdings for this security, and
				// then creating a new short position for the remaining balance of
				// this transaction.
			}
		 }
		else
		// This is a buy transaction, so estimate the impact to any currently held
		// short positions in the security. These are represented as negative H_QTY
		// holdings. Short postions will be covered before opening a long postion in
		// this security.
		{
			if (hold_qty <0) { // Existing short position to buy
				if (is_lifo) {
					// Estimates will be based on closing most recently acquired holdings
					// Could return 0, 1 or many rows
					holdList = dbObject.QUERY2LST(sqlTOF3_6a);
				}
				else // Estimates will be based on closing oldest holdings // Could return 0, 1 or many rows
				{
					holdList = dbObject.QUERY2LST(sqlTOF3_6b);
				}
			// Estimate, based on the requested price, any profit that may be realized
			// by covering short postions currently held for this security. The customer
			// may have multiple holdings at different prices for this security
			// (representing multiple purchases at different times).
				if (!holdList.isEmpty()){
					for(int i=0; i<holdList.size(); i++){
						Map entry = (Map) holdList.get(i);
						if ((int) entry.get("h_qty") + needed_qty < 0){
							sell_value += needed_qty * (double) entry.get("h_price");
							buy_value += needed_qty * trade_price;
							needed_qty = 0;
							continue;
						}
						else {
							hold_qty = -hold_qty;
							sell_value += hold_qty * (double) entry.get("h_price");
							buy_value += hold_qty * trade_price;
							needed_qty = needed_qty - (int) entry.get("h_qty");
						}
					}
				}
			}
		}
		/*String  SQLTOF3_7 = String.format(
				"SELECT sum(tx_rate) " +
						"FROM taxrate " +
						"WHERE tx_id in ( " +
						"                SELECT cx_tx_id " +
						"                FROM customer_taxrate " +
						"                WHERE cx_c_id = %ld) ",);

		String  SQLTOF3_8 = String.format(
				"SELECT cr_rate " +
						"FROM commission_rate " +
						"WHERE cr_c_tier = %d " +
						"  AND cr_tt_id = '%s' " +
						"  AND cr_ex_id = '%s' +n" +
						"  AND cr_from_qty <= %d " +
						"  AND cr_to_qty >= %d",);

		String  SQLTOF3_9 = String.format(
				"SELECT ch_chrg " +
						"FROM charge " +
						"WHERE ch_c_tier = %d " +
						"  AND ch_tt_id = '%s' ", );

		String  SQLTOF3_10 = String.format(
				"SELECT ca_bal " +
						"FROM customer_account " +
						"WHERE ca_id = %s",);

		String  SQLTOF3_11 = String.format(
				"SELECT sum(hs_qty * lt_price) " +
						"FROM holding_summary, last_trade " +
						"WHERE hs_ca_id = %ld " +
						"  AND lt_s_symb = hs_s_symb",);*/


		// Set the status for this trade
		String status_id = "";
		if (type_is_market) {status_id = "SBMT"; }
		else {status_id = "PNDG";}

		//charge amount and comm amount are set just like that
		// we will calculate them later, if we have time
		String charge_amount = "10.60";
		String comm_amount = "0.70";
		String tradePriceStr = Double.toString(trade_price).replace(",", ".");
		//String is_lifo_str  = is_lifo ? "1" :"0";

		String  sqlTOF4_1 = String.format(
				"INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, " +
						"                  t_s_symb, t_qty, t_bid_price, t_ca_id, " +
						"                  t_exec_name, t_trade_price, t_chrg, t_comm, " +
						"                  t_tax, t_lifo) " +
						"VALUES (NEXTVAL('seq_trade_id'), now(), '%s', '%s', %s, '%s', " +
						"        %d, %s, %s, '%s', NULL, %s, %s, 0, %s) ",
						 status_id, t_tt_id, t_is_cash, symbol, trade_qty, tradePriceStr, acct_id,
				exec_name, charge_amount, comm_amount, is_lifo);

		dbObject.DML(sqlTOF4_1);
		//:USE EXEC_QUERY
		String trade_id = dbObject.EXEC_QUERY("SELECT CURRVAL('SEQ_TRADE_ID')");

		String  sqlTOF4_2 = String.format(
				"INSERT INTO trade_request(tr_t_id, tr_tt_id, tr_s_symb, tr_qty, " +
						"                          tr_bid_price, tr_b_id) " +
						"VALUES (%s, '%s', '%s', %d, %8.2f, %s)",trade_id, t_tt_id, symbol, trade_qty, trade_price,
				broker_id);
		dbObject.DML(sqlTOF4_2);

		String  sqlTOF4_3 = String.format(
				"INSERT INTO trade_history(th_t_id, th_dts, th_st_id) " +
						"VALUES(%s, now(), '%s')", trade_id, status_id);
		dbObject.DML(sqlTOF4_3);
		dbObject.TCL("commit", "tradeOrder");
		s.insertTime(9, System.currentTimeMillis() - t);
        //s.txnMix[9] = s.txnMix[9] + System.currentTimeMillis() - t;
		// Invoke tradeResult before exiting method.
		tradeResult(dbObject, s, trade_id, trade_price);
		s.increment(4);
		//s.txnMix[4]++;
		//System.out.printf("Trade Id: %s \n Trade Price: %f", trade_id, trade_price);
	}

	private static void tradeResult(arisDemo dbObject, Statistics s, String trade_id, double trade_price){
		Long t = System.currentTimeMillis();
		dbObject.START_TX("tradeResult");
		String trFrame1_1 = String.format(
				"SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg " +
						"FROM trade " +
						"WHERE t_id = %s", trade_id);
		Map output1 = dbObject.QUERY2MAP(trFrame1_1);
		String acct_id 		= output1.get("t_ca_id").toString();
		String type_id 		= output1.get("t_tt_id").toString();
		String symbol 		= output1.get("t_s_symb").toString();
		String trade_qty 	= output1.get("t_qty").toString();
		String charge		= output1.get("t_chrg").toString();


		String trFrame1_2 = String.format(
				"SELECT tt_name " +
				"FROM trade_type " +
				"WHERE tt_id = '%s'", type_id);
		dbObject.QUERY(trFrame1_2);
		String trFrame1_3 = String.format(
				"SELECT hs_qty " +
						"FROM holding_summary " +
						"WHERE hs_ca_id = %s " +
						"  AND hs_s_symb = '%s'", acct_id, symbol);
		Map output1a = dbObject.QUERY2MAP(trFrame1_3);
		String hold_qty = "";
		if (output1a.containsKey("h_qty")){
			 hold_qty = output1a.get("h_qty").toString();
		}else{
			hold_qty = "0";
		}

		// Initialize variables
		Date date= new Date();
		Date trade_date = new Timestamp(date.getTime());
		String trade_dts = trade_date.toString();
		double buy_value = 0.0;
		double sell_value = 0.0;
		boolean type_is_sell;
		int needed_qty = Integer.parseInt(trade_qty);
		switch (type_id) {
			case "TMS": type_is_sell = true;
				break;
			case "TLS": type_is_sell = true;
				break;
			default: type_is_sell = false;
		}


		String trFrame2_1 = String.format(
				"SELECT ca_b_id, ca_c_id, ca_tax_st " +
				"FROM customer_account " +
				"WHERE ca_id = %s " +
				"FOR UPDATE", acct_id);
		Map initFrame3 = dbObject.QUERY2MAP(trFrame2_1);

		List holdList =new ArrayList<Map<String, Object>>();
		if (type_is_sell){
			if (Integer.parseInt(hold_qty ) == 0) {
				String trFrame2_2a = String.format(
						"INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
								"VALUES(%s, '%s', %d)", acct_id, symbol, (-1)*Integer.parseInt(trade_qty));
				dbObject.DML(trFrame2_2a);
			}
			else if (Integer.parseInt(hold_qty)!=Integer.parseInt(trade_qty)) {
				String trFrame2_2b = String.format(
						"UPDATE holding_summary " +
								"SET hs_qty = %d " +
								"WHERE hs_ca_id = %s  " +
								"  AND hs_s_symb = '%s'", Integer.parseInt(hold_qty)-Integer.parseInt(trade_qty),
						acct_id, symbol);
				dbObject.DML(trFrame2_2b);
			}
			//sell trade:
			// First look for existing holdings, H_QTY >
			if (Integer.parseInt(hold_qty ) > 0) {
				//skip checking of is_lifo;
				String trFrame2_3a = String.format(
						"SELECT h_t_id, h_qty, h_price " +
								"FROM holding " +
								"WHERE h_ca_id = %s " +
								"  AND h_s_symb = '%s' " +
								"ORDER BY h_dts DESC ", acct_id, symbol);
				holdList = dbObject.QUERY2LST(trFrame2_3a);
				if (!holdList.isEmpty()){
					// Liquidate existing holdings. Note that more than
					// 1 HOLDING record can be deleted here since customer
					// may have the same security with differing prices.
					for(int i=0; i<holdList.size(); i++){
						//System.out.println(holdList.get(i));
						Map entry = (Map) holdList.get(i);
						if ((int) entry.get("h_qty") > needed_qty){
							//Selling some of the holdings
							String trFrame2_4a = String.format(
									"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
											"                            hh_after_qty) " +
											"VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, entry.get("h_qty"),
									(Integer.parseInt(hold_qty )- needed_qty));
							dbObject.DML(trFrame2_4a);

							String trFrame2_5a = String.format(
									"UPDATE holding " +
											"SET h_qty = %d " +
											"WHERE h_t_id = %s", Integer.parseInt(hold_qty)-needed_qty, entry.get("h_t_id"));
							dbObject.DML(trFrame2_5a);
							buy_value += needed_qty * (double) entry.get("h_price");
							sell_value += needed_qty * trade_price;
							needed_qty = 0;
							continue;
						}
						else {
							//selling all holdings
							String trFrame2_4a = String.format(
									"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
											"                            hh_after_qty) " +
											"VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, entry.get("h_qty"), 0);
							dbObject.DML(trFrame2_4a);
							String trFrame2_5b = String.format(
									"DELETE FROM holding " +
											"WHERE h_t_id = %s", entry.get("h_t_id"));
							dbObject.DML(trFrame2_5b);

							buy_value += Integer.parseInt(hold_qty) * (double) entry.get("h_price");
							sell_value += Integer.parseInt(hold_qty) * trade_price;
							needed_qty = needed_qty - (int) entry.get("h_qty");
						}
					}//close
				}

				//Map output2 = dbObject.QUERY2MAP(trFrame2_3a);
				//String hold_id 		= output2.get("h_t_id").toString();
				//String hold_qty 	= output2.get("h_qty").toString();
				//String hold_price 	= output2.get("h_price").toString();
			}
			// Sell Short:
			// If needed_qty > 0 then customer has sold all existing
			// holdings and customer is selling short. A new HOLDING
			// record will be created with H_QTY set to the negative
			// number of needed shares.
			if (needed_qty > 0) {
				String trFrame2_4a = String.format(
						"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
								"                            hh_after_qty) " +
								"VALUES(%s, %s, %s, %d)", trade_id, trade_id, 0, (-1)*needed_qty);
				dbObject.DML(trFrame2_4a);

                String checkUpdate = String.format("Select count(*) from holding_summary where " +
                        "hs_s_symb =" +
                        " '%s' and hs_ca_id = %s", symbol, acct_id);
                String count = dbObject.QUERY2MAP(checkUpdate).get("count").toString();
                if (Integer.parseInt(count) < 1) {
                    String trFrame2_update = String.format("insert into holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
                            "VALUES (%s, '%s', %d)", acct_id, symbol, (-1) * needed_qty);
                    dbObject.DML(trFrame2_update);
                }

				String trFrame2_7a = String.format(
						"INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, " +
								"                    h_qty) " +
								"VALUES (%s, %s, '%s', '%s', %f, %d)",
						trade_id, acct_id, symbol, trade_dts, trade_price, (-1)*needed_qty);
				dbObject.DML(trFrame2_7a);
			}
			else if (hold_qty == trade_qty) {
				String trFrame2_7b = String.format(
						"DELETE FROM holding_summary " +
								"WHERE hs_ca_id = %s " +
								"  AND hs_s_symb = '%s'", acct_id, symbol);
				dbObject.DML(trFrame2_7b);
			}

		}//end if type_is_sell
		else { //the trade is a BUY
			if (Integer.parseInt(hold_qty) == 0) {  //no prior holdings exist, but one will be inserted
				String trFrame2_8a = String.format(
						"INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
								"VALUES (%s, '%s', %s)", acct_id, symbol, trade_qty);
				dbObject.DML(trFrame2_8a);
			}
			else {  //if (Integer.parseInt(hold_qty) != 0)
				if ((-1)*Integer.parseInt(hold_qty) != Integer.parseInt(trade_qty)){
					String trFrame2_8b = String.format(
							"UPDATE holding_summary " +
									"SET hs_qty = %s " +
									"WHERE hs_ca_id = %s " +
									"  AND hs_s_symb = '%s'", trade_qty, acct_id, symbol);
					dbObject.DML(trFrame2_8b);
				}
			}//end if hold_qty == 0
			// Short Cover:
			// First look for existing negative holdings, H_QTY < 0,
			// which indicates a previous short sell. The buy trade
			// will cover the short sell.
			if (Integer.parseInt(hold_qty) < 0) {
				//skip checking of is_lifo;
				String trFrame2_3a = String.format(
						"SELECT h_t_id, h_qty, h_price " +
								"FROM holding " +
								"WHERE h_ca_id = %s " +
								"  AND h_s_symb = '%s' " +
								"ORDER BY h_dts DESC ", acct_id, symbol);
				holdList = dbObject.QUERY2LST(trFrame2_3a);
				// Buy back securities to cover a short position. open hold_list
				for(int i=0; i<holdList.size(); i++){
					Map entry = (Map) holdList.get(i);
					if ((int) entry.get("h_qty") + needed_qty < 0) {
						//Bying back some of the short sell
						String trFrame2_4a = String.format(
								"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
										"                            hh_after_qty) " +
										"VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, 0, (int) entry.get
										("h_qty") + needed_qty);
						dbObject.DML(trFrame2_4a);
						String trFrame2_5a = String.format(
								"UPDATE holding " +
										"SET h_qty = %d " +
										"WHERE h_t_id = %s", Integer.parseInt(hold_qty)+needed_qty, entry.get("h_t_id"));
						dbObject.DML(trFrame2_5a);
						sell_value 	+= needed_qty*(int)entry.get("h_price");
						buy_value 	+= needed_qty*trade_price;
						needed_qty 	= 0;
						break;
					}
					else { //buying back all of the short sell
						String trFrame2_4a = String.format(
								"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) " +
										"VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, entry.get("h_qty"), 0);
						dbObject.DML(trFrame2_4a);
						String trFrame2_5b = String.format(
								"DELETE FROM holding " +
										"WHERE h_t_id = %s", entry.get("h_t_id"));
						dbObject.DML(trFrame2_5b);
						// Make hold_qty positive for easy calculations
						hold_qty = "-"+hold_qty;
						sell_value += Integer.parseInt(hold_qty) * (int) entry.get("h_price");
						buy_value += Integer.parseInt(hold_qty) * trade_price;
						needed_qty = needed_qty - Integer.parseInt(hold_qty);
					}
				}//end for loop over hold_list
			}
			// Buy Trade:
			// If needed_qty > 0, then the customer has covered all
			// previous Short Sells and the customer is buying new
			// holdings. A new HOLDING record will be created with
			// H_QTY set to the number of needed shares.
			if (needed_qty > 0) {
				String trFrame2_4a = String.format(
						"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) " +
								"VALUES(%s, %s, %s, %d)", trade_id, trade_id, 0, needed_qty);
				dbObject.DML(trFrame2_4a);

                String checkUpdate = String.format("Select count(*) from holding_summary where " +
                        "hs_s_symb =" +
                        " '%s' and hs_ca_id = %s", symbol, acct_id);
                String count = dbObject.QUERY2MAP(checkUpdate).get("count").toString();
                if (Integer.parseInt(count) < 1) {
                    String trFrame2_update = String.format("insert into holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
                            "VALUES (%s, '%s', %d)", acct_id, symbol, needed_qty);
                    dbObject.DML(trFrame2_update);
                }

				String trFrame2_7a = String.format(
						"INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, " +
								"                    h_qty) " +
								"VALUES (%s, %s, '%s', '%s', %f, %d)",
						trade_id, acct_id, symbol, trade_dts, trade_price, needed_qty);
				dbObject.DML(trFrame2_7a);
			}
			else if (Integer.parseInt(hold_qty )== Integer.parseInt(trade_qty)){
				String trFrame2_5b = String.format(
						"DELETE FROM holding_summary " +
								"WHERE h_ca_id = %s and hs_s_symb = '%s'", acct_id, symbol);
				dbObject.DML(trFrame2_5b);
			}
		}//end buy

		//FRAME 3
		String broker_id 	= initFrame3.get("ca_b_id").toString();
		String cust_id 		= initFrame3.get("ca_c_id").toString();
		String tax_status 	= initFrame3.get("ca_tax_st").toString();
		//init tax_ammount to 0.0
		double tax_amount = 0.0;


		String trFrame3_1 = String.format(
				"SELECT SUM(tx_rate) " +
						"FROM taxrate " +
						"WHERE tx_id IN (SELECT cx_tx_id " +
						"                FROM customer_taxrate " +
						"                WHERE cx_c_id = %s) ", cust_id);
		double tax_rates = Double.parseDouble(dbObject.QUERY2MAP(trFrame3_1).get("sum").toString());
		tax_amount = (sell_value - buy_value) * tax_rates;

		String trFrame3_2 = String.format(
				"UPDATE trade " +
				"SET t_tax = %f " +
				"WHERE t_id = %s", tax_amount, trade_id);
		dbObject.DML(trFrame3_2);

		//Finished frame 3.
		//Go for frame 4..


		String trFrame4_1 = String.format(
				"SELECT s_ex_id, s_name " +
						"FROM security " +
						"WHERE s_symb = '%s'", symbol);
		//System.out.println(trFrame4_1);
		Map sexid = dbObject.QUERY2MAP(trFrame4_1);
		String s_ex_id = sexid.get("s_ex_id").toString();
		String s_name = sexid.get("s_name").toString();

		String trFrame4_2 = String.format(
				"SELECT c_tier " +
						"FROM customer " +
						"WHERE c_id = %s", cust_id);
		Map ctier = dbObject.QUERY2MAP(trFrame4_2);
		String c_tier = ctier.get("c_tier").toString();

		String trFrame4_3 = String.format(
				"SELECT cr_rate " +
						"FROM commission_rate " +
						"WHERE cr_c_tier = %s " +
						"  AND cr_tt_id = '%s' " +
						"  AND cr_ex_id = '%s' " +
						"  AND cr_to_qty-cr_from_qty >= %s " +
						"  AND cr_to_qty >= %s " +
						"LIMIT 1", c_tier, type_id, s_ex_id, trade_qty, trade_qty);
		//Map crrate = dbObject.QUERY2MAP(trFrame4_3);
		String comm_rate = dbObject.QUERY2MAP(trFrame4_3).get("cr_rate").toString();
		//double comm_rate = (double) crrate.get("cr_rate");
		double comm_amount = (Double.parseDouble(comm_rate)/ 100) * (Integer.parseInt(trade_qty )*  trade_price);

		//END OF FRAME 4
		//GO FOR FRAME 5
		String st_completed_id = "CMPT";
		String trFrame5_1 = String.format(
				"UPDATE trade " +
				"SET t_comm = %f, " +
				"    t_dts = '%s', " +
				"    t_st_id = '%s', " +
				"    t_trade_price = %f " +
				"WHERE t_id = %s", comm_amount, trade_dts, st_completed_id, trade_price, trade_id);
		dbObject.DML(trFrame5_1);

		String trFrame5_2 = String.format(
				"INSERT INTO trade_history(th_t_id, th_dts, th_st_id) " +
				"VALUES (%s, '%s', '%s')", trade_id, trade_dts, st_completed_id);
		dbObject.DML(trFrame5_2);

		String trFrame5_3 = String.format(
				"UPDATE broker " +
				"SET b_comm_total = b_comm_total + %f, " +
				"    b_num_trades = b_num_trades + 1 " +
				"WHERE b_id = %s", comm_amount, broker_id);
		dbObject.DML(trFrame5_3);

		//END OF FRAME 5
		//GO FOR FRAME 6
		boolean cash_type = ThreadLocalRandom.current().nextBoolean();
		Date due_txn_date = org.apache.commons.lang3.time.DateUtils.addDays(trade_date, 2);
		String due_date = due_txn_date.toString();
		double se_amount = 0.0;
		if (type_is_sell) {
			se_amount = (Integer.parseInt(trade_qty)*trade_price) - Double.parseDouble(charge) - comm_amount;
		} else {
			se_amount = -(Integer.parseInt(trade_qty)*trade_price) + Double.parseDouble(charge) + comm_amount;
		}
		if (tax_status == "1") {
			se_amount = se_amount - tax_amount;
		}

		String trFrame6_1 = String.format(
				"INSERT INTO settlement(se_t_id, se_cash_type, se_cash_due_date,  " +
				"                       se_amt) " +
				"VALUES (%s, '%s', '%s', %f)", trade_id, cash_type, due_date, se_amount);
		dbObject.DML(trFrame6_1);

		String trFrame6_2 = String.format(
				"UPDATE customer_account " +
				"SET ca_bal = ca_bal + %f " +
				"WHERE ca_id = %s", se_amount, acct_id);
		dbObject.DML(trFrame6_2);

		String type_name = "";
		switch(type_id){
			case "TMS": type_name = "Market-Sell";
				break;
			case "TMB": type_name = "Market-Buy";
				break;
			case "TLS": type_name = "Limit-Sell";
				break;
			case "TLB": type_name = "Limit- Buy";
				break;
			default: type_name = "Stop-Loss";
		}

		String trFrame6_3 = String.format(
				"INSERT INTO cash_transaction(ct_dts, ct_t_id, ct_amt, ct_name) " +
				"VALUES ('%s', %s, %f, e'%s %s shared of %s')", trade_dts, trade_id, se_amount, type_name,
				trade_qty, s_name);
		dbObject.DML(trFrame6_3);

		String trFrame6_4 = String.format(
				"SELECT ca_bal " +
				"FROM customer_account " +
				"WHERE ca_id = %s", acct_id);
		dbObject.QUERY(trFrame6_4);
		dbObject.TCL("commit", "tradeResult");
		s.insertTime(10, System.currentTimeMillis() - t);
		//s.txnMix[10] = s.txnMix[10] + System.currentTimeMillis() - t;
	}

	private static void tradeStatus(arisDemo dbObject, Statistics s) {
		String acct_id =  all_acct_ids.get(ThreadLocalRandom.current().nextInt(0, all_acct_ids.size())).toString();
		Long t = System.currentTimeMillis();
		String  sqlTSF1_1 = String.format(
				"SELECT t_id, t_dts, st_name, tt_name, t_s_symb, t_qty, " +
				"       t_exec_name, t_chrg, s_name, ex_name "+
				"FROM trade, status_type, trade_type, security, exchange "+
				"WHERE t_ca_id = %s " +
				"  AND st_id = t_st_id " +
				"  AND tt_id = t_tt_id " +
				"  AND s_symb = t_s_symb " +
				"  AND ex_id = s_ex_id " +
				"ORDER BY t_dts DESC " +
				"LIMIT 50", acct_id);
		dbObject.QUERY(sqlTSF1_1);

		String  sqlTSF1_2 = String.format(
		"SELECT c_l_name, c_f_name, b_name " +
		"FROM customer_account, customer, broker " +
		"WHERE ca_id = %s " +
		"  AND c_id = ca_c_id " +
		"  AND b_id = ca_b_id", acct_id);
		dbObject.QUERY(sqlTSF1_2);
		s.insertTime(11, System.currentTimeMillis() - t);
		//s.txnMix[11] = s.txnMix[11] + System.currentTimeMillis() - t;

	}

	private static void tradeUpdate(arisDemo dbObject) {
		/*List tradeIdsList = dbObject.QUERY2LST("select t_id from trade");


		Long t = System.currentTimeMillis();

		String SQLTUF1_1 = String.format(
				"SELECT t_exec_name " +
						"FROM trade " +
						"WHERE t_id = %ld", );

		String SQLTUF1_2a = String.format(
				"SELECT REPLACE('%s', ' X ', ' ')", );

		String SQLTUF1_2b = String.format(
				"SELECT REPLACE('%s', ' ', ' X ')", );

		String SQLTUF1_3 = String.format(
				"UPDATE trade " +
						"SET t_exec_name = '%s' " +
						"WHERE t_id = %ld", );

		String SQLTUF1_4 = String.format(
				"SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt, " +
						"       t_trade_price " +
						"FROM trade, trade_type " +
						"WHERE t_id = %ld " +
						"  AND t_tt_id = tt_id", );

		String SQLTUF1_5 = String.format(
				"SELECT se_amt, se_cash_due_date, se_cash_type " +
						"FROM settlement " +
						"WHERE se_t_id = %ld", );

		String SQLTUF1_6 = String.format(
				"SELECT ct_amt, ct_dts, ct_name " +
						"FROM cash_transaction " +
						"WHERE ct_t_id = %ld", );

		String SQLTUF1_7 = String.format(
				"SELECT th_dts, th_st_id " +
						"FROM trade_history " +
						"WHERE th_t_id = %ld " +
						"ORDER BY th_dts " +
						"LIMIT 3", );

		String SQLTUF2_1 = String.format(
				"SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price " +
						"FROM trade " +
						"WHERE t_ca_id = %ld " +
						"  AND t_dts >= '%s' " +
						"  AND t_dts <= '%s' " +
						"ORDER BY t_dts ASC " +
						"LIMIT %d", );

		String SQLTUF2_2 = String.format(
				"SELECT se_cash_type " +
						"FROM settlement " +
						"WHERE se_t_id = %s", );

		String SQLTUF2_3 = String.format(
				"UPDATE settlement " +
						"SET se_cash_type = '%s' " +
						"WHERE se_t_id = %s", );

		String SQLTUF2_4 = String.format(
				"SELECT se_amt, se_cash_due_date, se_cash_type " +
						"FROM settlement " +
						"WHERE se_t_id = %s", );

		String SQLTUF2_5 = String.format(
				"SELECT ct_amt, ct_dts, ct_name " +
						"FROM cash_transaction " +
						"WHERE ct_t_id = %s", );

		String SQLTUF2_6 = String.format(
				"SELECT th_dts, th_st_id " +
						"FROM trade_history " +
						"WHERE th_t_id = %s " +
						"ORDER BY th_dts " +
						"LIMIT 3", );

		String SQLTUF3_1 = String.format(
				"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty, " +
						"       s_name, t_dts, t_id, t_tt_id, tt_name " +
						"FROM trade, trade_type, security " +
						"WHERE t_s_symb = '%s' " +
						"  AND t_dts >= '%s' " +
						"  AND t_dts <= '%s' " +
						"  AND tt_id = t_tt_id " +
						"  AND s_symb = t_s_symb " +
						"ORDER BY t_dts ASC " +
						"LIMIT %d", );

		String SQLTUF3_2 = SQLTUF2_4;

		String SQLTUF3_3 = String.format(
				"SELECT ct_name " +
						"FROM cash_transaction " +
						"WHERE ct_t_id = %s", );

		String SQLTUF3_4 = String.format(
				"UPDATE cash_transaction " +
						"SET ct_name = e'%s' " +
						"WHERE ct_t_id = %s", );

		String SQLTUF3_5 = SQLTUF2_5;

		String SQLTUF3_6 = SQLTUF2_6;

		TU_Durations.add(System.currentTimeMillis() - t);
		*/
	}

	private static class SimTest implements Callable<String> {
		private Statistics s;

		SimTest(Statistics stats){
			this.s = stats;
		}

		@Override
        //Changed Object to String
        public String call() throws Exception {

            //System.out.println(d.setConsistency("set consistency level 1"));

            long lStartTime = System.currentTimeMillis();
            int i =0;

            List txnsToRun  = new Vector<String>();
            txnsToRun = workloadMix(MIXSELECTOR);
 			/*
            while(i < txnsToRun.size()){
				generateTxn(d,txnsToRun.get(i).toString(), s);
				i++;
			}*/
            //Code changed to support timed out threads
            while (!Thread.interrupted() && i< txnsToRun.size()) {
                arisDemo d = new arisDemo();
                d.CONNECT();
                generateTxn(d,txnsToRun.get(i).toString(), s);
                d.DISCONNECT();
                i++;
               /*
               if (i >= txnsToRun.size()) {
                    i = 0;
                }
                */
            }
            long lEndTime = System.currentTimeMillis();
            long dTime = lEndTime - lStartTime;

            //System.out.println("Connection closed from thread: "+ Thread.currentThread().getName());
            return "Connection closed from thread: "+ Thread.currentThread().getName();
        }
        public String callUnused() throws Exception {
			arisDemo d = new arisDemo();
			String session_id = d.CONNECT();
			//System.out.println(d.setConsistency("set consistency level 1"));

			long lStartTime = System.currentTimeMillis();
			int i =0;

            List txnsToRun  = new Vector<String>();
            txnsToRun = workloadMix(MIXSELECTOR);
 			/*
            while(i < txnsToRun.size()){
				generateTxn(d,txnsToRun.get(i).toString(), s);
				i++;
			}*/
            //Code changed to support timed out threads
            while (!Thread.interrupted()) {
                generateTxn(d,txnsToRun.get(i).toString(), s);
                i++;

               if (i >= txnsToRun.size() - 1) {
                    i = 0;
                }
            }
			long lEndTime = System.currentTimeMillis();
			long dTime = lEndTime - lStartTime;
			d.DISCONNECT();
			System.out.println("Connection closed from thread: "+ Thread.currentThread().getName());
			return session_id+" completed in "+dTime+" msec.";
		}
	}
}

