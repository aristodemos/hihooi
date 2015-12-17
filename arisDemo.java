package hih;
//edit from github 2
//import com.sun.tools.javac.code.Attribute;

import org.omg.PortableServer.THREAD_POLICY_ID;
import org.xml.sax.SAXParseException;

import java.io.*;
//import java.lang.reflect.Array;
import java.nio.channels.InterruptedByTimeoutException;
import java.sql.*;
//import java.text.DateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.Arrays;
import java.text.SimpleDateFormat;
//import java.util.Calendar;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

public class arisDemo {


	static final class Statistics {
		private long[] txnMix = new long[2*(txnPoolMaster.size()+1)];
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
			txnMix[i]=txnMix[i]+timeInterval;
			//}
		}

		public long txnsPerSec(){
			return txnMix[0]+txnMix[1]+txnMix[2]+txnMix[3]+txnMix[4]+txnMix[5]+txnMix[6];
		}
		public long totalOps(){
			return totalOps;
		}
        public long totalWriteOps(){
            return writeOps;
        }
	}

	//private String LISTENER="54.201.96.59";
	private String LISTENER="172.30.0.130";
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
	public String setConsistency(int setCMD) {
		/*
		hih.set(“set consistency level 1”)etc.
			set consistency level 2;
			set consistency level 3;
			set consistency level 4;
		*/
		return hih.set("set consistency level "+setCMD);
	}

	public String DISCONNECT()
	{
		return hih.disconnect();  //Returns "Disconnect Successfuly"
	}

	//GLOBALS used to generate the txns for the benchmark
	public static List all_brokers 						= new Vector(10);
	public static List<String> all_sectors 				= new Vector<String>(12);
	public static List<String> all_customers			= new Vector<String>(1000);
	public static List<String> all_symbols 				= new Vector<String>(685);
	public static List activeSymbols 					= new Vector();
	private static List all_acct_ids 					= new Vector(5000);
	public static Map<String, List<Double>> pricesDM 	= new HashMap<String, List<Double>>();

	//public static int trxnsPerSession   = 10;
	public static int       SESSIONS        = 32; //threads to spawn (on the machine where this program is run)
	public static int       TIMETORUN       = 15; //in minutes
	public static String    MIXSELECTOR   	= "d"; // a,b,c,d    default: all transactions (d)
	private static boolean  DEBUG           = false; //print transactions to file and other msgs on system.out
	private static String   LAST_T_ID       = "200000000290880";
	private static int 	    MODE		    = 1; //1, 2, 3, 4
	private static boolean 	BYPASS			= false; //run direclty on postgres. stopAll and start postgres normally (pg_ctl)

	//The writer for the Log
	public static PrintWriter logWriter = null;

	//The pool of transactions - we choose one at random
	public static List<String> txnPoolMaster = Arrays.asList("BrokerVolume", "CustomerPosition", "MarketFeed",
			"TradeOrder", "TradeStatus", "SecurityDetail");

	//Atomic Array of Long to save the total latency(cummulative) and total number of each transaction
	//private static  long[] txnMix = new long[2*(txnPoolMaster.size()+1)];

	static  Statistics stats = new Statistics();

	public String EXEC_QUERY(String SQL) {
		if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		List<Map<String, Object>> rows = hih.executeQuery(SQL);
		String output ="";
		for( int i = rows.size() -1; i >= 0 ; i --) {
			Map<String,Object> entry = rows.get(i);
			for (String key : entry.keySet()) {
				output = ""+entry.get(key);
			}
		}
		if (DEBUG){System.out.println(output);}
		stats.incOperation();
		return output;
	}
	private final ExecutorService futPool = Executors.newFixedThreadPool(10);

	public Future<List> QUERYF(final String SQL) {
		return futPool.submit(new Callable<List>() {
			@Override
			public List call()  {
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
				if (DEBUG){System.out.println(resultOut);}
				return resultOut;
			}
		});
	}

	public List QUERY(String SQL) {
		if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
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
		if (DEBUG){System.out.println(resultOut);}
        stats.incOperation();
		return resultOut;
	}


	public String QUERY2STR(String SQL){
		if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
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


	public Future<Map> QUERYF2MAP(final String SQL) {
		return futPool.submit(new Callable<Map>() {
			@Override
			public Map call()  {
				List<Map<String, Object>> rows = hih.executeQuery(SQL);
				Map<String, Object> results = new HashMap<>();
				for( int i = rows.size() -1; i >= 0 ; i --) {
					Map<String,Object> entry = rows.get(i);
					for (String key : entry.keySet()){
						results.put(key, entry.get(key));
					}
				}
				if (DEBUG){System.out.println(results);}
				return results;
			}
		});
	}


	public Map QUERY2MAP(String SQL){
		if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
		List<Map<String, Object>> rows = hih.executeQuery(SQL);
		Map<String, Object> results = new HashMap<>();
		for( int i = rows.size() -1; i >= 0 ; i --) {
			Map<String,Object> entry = rows.get(i);
			for (String key : entry.keySet()){
				results.put(key, entry.get(key));
			}
		}
		if (DEBUG){System.out.println(results);}
        stats.incOperation();
		return results;
	}


	public Future<List> QUERYF2LST(final String SQL) {
		return futPool.submit(new Callable<List>() {
			@Override
			public List call()  {
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
				if (DEBUG){System.out.println(rows);}
				return rows;
			}
		});
	}


	public List QUERY2LST(String SQL){
		if (DEBUG ){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
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
		if (DEBUG){System.out.println(rows);}
        stats.incOperation();
		return rows;
	}

	//Data Manipulation Language: INSERT, DELETE, UPDATE;
	public String DML(String SQL) {
		if (DEBUG){logWriter.printf("%s : %d \n", SQL, System.currentTimeMillis());}
        stats.incWriteOp();
		return hih.executeUpdate(SQL);
	}

	public String START_TX() {
        stats.incWriteOp();
		return hih.startTransaction();
	}
	//TRANSACTION CONTROL LANGUAGE: COMMIT, ROLLBACK;
	public String TCL(String tcl_cmd) {
		if (tcl_cmd.equalsIgnoreCase("commit")) {
            stats.incOperation();
			return hih.commitTransaction();
		}
		else {
            stats.incOperation();
			return hih.rollbackTransaction();
		}
	}

	public arisDemo() {}
	static ExecuteShellCommand shell = new ExecuteShellCommand();

	public static void main(String [] args) throws IOException {
		if (args.length > 0) {
			for (int i=0; i< args.length; i++) {
				if (args[i].equalsIgnoreCase("-sessions") ||args[i].equalsIgnoreCase("-s")) {
					SESSIONS = Integer.parseInt(args[i + 1]);
				}
				if (args[i].equalsIgnoreCase("-mix") || args[i].equalsIgnoreCase("-m")) {
					MIXSELECTOR = args[i + 1];
				}
				if (args[i].equalsIgnoreCase("-time") || args[i].equalsIgnoreCase("-t")) {
					TIMETORUN = Integer.parseInt(args[i + 1]);
				}
				if (args[i].equalsIgnoreCase("-op") || args[i].equalsIgnoreCase("-o")) {
					MODE = Integer.parseInt(args[i + 1]);
				}
				if (args[i].equalsIgnoreCase("-debug") || args[i].equalsIgnoreCase("-d")) {
					DEBUG = true;
				}
				if (args[i].equalsIgnoreCase("-bypass") || args[i].equalsIgnoreCase("-b")) {
					BYPASS = true;
				}
			}
		}
		if (BYPASS){
			MIXSELECTOR = "a";
			System.out.println("note: When bypassing hihooi read-only workload is used");
		}

		//Create transaction Log File
		//Remember to close it
		String timeStamp = new SimpleDateFormat("yy.MM.dd.HH.mm.ss").format(new java.util.Date());
		try{
			logWriter = new PrintWriter(timeStamp+"_"+MIXSELECTOR+"_"+MODE+".txt", "UTF-8");

		}catch (FileNotFoundException |UnsupportedEncodingException err){
			System.out.println("FileNOTfound" + err.getMessage());
		}

		Locale.setDefault(Locale.US);
		System.out.println("****************** Test Run Parameters *********************");
		System.out.println("************************************************************");
		System.out.println("Number of Sessions: "+SESSIONS);
		System.out.println("Test Duration (in minutes): "+TIMETORUN);
		System.out.println("Using Mix: " + MIXSELECTOR);
		System.out.println("Debug is set to: "+DEBUG);
		System.out.println("Bypass is set to: "+BYPASS);
		//System.out.println("Last Trade Id "+LAST_T_ID);
		System.out.println("Mode = " +MODE);

		//init Statistics
		//final Statistics stats = new Statistics();

		System.out.println("*************** Initializing the Test Run ******************");
		//arisDemo d = new arisDemo();
		//System.out.println(d.CONNECT());
		//System.out.println("Connection Established");

		//System.out.println("Cleaning Up database");
		//preInitRun(d);

		//Gather necessary data from the initialized database;
		System.out.println("Initializing Parameters");
		initParams();
		//System.out.println("Disconnecting after initialization...");
		//System.out.println(d.DISCONNECT());

		System.out.println("Starting Sessions");
		System.out.println("Number of threads (sessions):  "+ SESSIONS);
		//System.out.println("Txns per Session :  " + trxnsPerSession);
		//KEEP LOGWRITER CLEAN TO EASE CALCULATIONS
		//logWriter.printf("Number of threads (sessions):  " + SESSIONS + "\r\n");
		//logWriter.printf("Txns per Session :  " + trxnsPerSession+"\r\n");

		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		exec.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				// do stuff
				logWriter.print(stats.txnsPerSec() + "\r\n");
			}
		}, 0, 5, TimeUnit.SECONDS);

		long startTime = System.currentTimeMillis(); //fetch starting time
		ExecutorService pool = Executors.newFixedThreadPool(SESSIONS);
		//List<Future<String>> list = new ArrayList<Future<String>>();

		Collection<SimTest> collection = new ArrayList<>();
		for(int i=0; i< SESSIONS; i++){
			arisDemo connector = new arisDemo();
			SimTest task = new SimTest(stats, connector); //SimTest task = new SimTest(); //
			collection.add(task);
		}
		try{
			List<Future<String>> listF = pool.invokeAll(collection, TIMETORUN, TimeUnit.MINUTES);
			for(Future<String> fut: listF){
				try {
					System.out.println("Time for Session " + fut.get());
					fut.get();
				}
				catch(CancellationException ce) {
					//ce.printStackTrace();
					//System.out.println("A cancellation exception occurred");
					fut.cancel(true);
				}
                catch (InterruptedException ex) {
                    System.out.println("An interrupted exception occurred");
                    fut.cancel(true);
                }
				catch (ExecutionException e) {
					System.out.println("An execution exception occurred");
					e.printStackTrace();
					fut.cancel(true);
				}
				catch(Exception e){
					//e.printStackTrace();
					System.out.println("An exception occurred");
					fut.cancel(true);
				}
			}
		}catch(InterruptedException ie){
			ie.printStackTrace();
			System.out.println("outer Interrupted Exception");
			System.out.println("Interrupted Exception");
		}catch(CancellationException ce){
			ce.printStackTrace();
			System.out.println("outer Cancellation Exception");
		}
		pool.shutdownNow();
		System.out.println("Closing Connections . . .");
		for (Iterator iterator = collection.iterator(); iterator.hasNext();) {
			SimTest type = (SimTest) iterator.next();
			//System.out.println(type.d);
			//type.disconnectNow();
			type.onCancel();
		}
		System.out.println("Connections Closed.");
		/*while (!pool.isTerminated()) {
			//this can throw InterruptedException, you'll need to decide how to deal with that.
			//System.out.println("Awaiting pool termination");
			try{
				pool.awaitTermination(1,TimeUnit.MILLISECONDS);
				pool.shutdownNow();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}*/
		long endTime = System.currentTimeMillis(); //end time
		long duration = endTime - startTime;
		System.out.println(" ALL SESSIONS COMPLETED IN " +duration+" msec \n");
		System.out.println(" which equals " + (duration / (1000 * 60.0)) + " minutes \n");
		exec.shutdownNow();

		//TODO: check if the above piece of code works
		/*
		Callable<String> callable = new  SimTest(stats);
		for(int i=0; i< SESSIONS; i++){
			//submit Callable tasks to be executed by thread pool
			Future<String> future = pool.submit(callable);
			//add Future to the list, we can get return value using Future
			list.add(future);
		}
		pool.shutdown();

		for(Future<String> fut : list) {
			try {
				System.out.println("Time for Session "+fut.get());
			}
			catch(Exception e) {
				e.printStackTrace();
				System.out.println("An exception occurred");
			}
		}
		pool.shutdownNow();
		while (!pool.isTerminated()) {
			//this can throw InterruptedException, you'll need to decide how to deal with that.
			try{pool.awaitTermination(1,TimeUnit.MILLISECONDS);}
			catch(Exception e){
				e.printStackTrace();
				System.out.println("An other exception occurred");
			}
		}
		long endTime = System.currentTimeMillis(); //end time
		long duration = endTime - startTime;
		*/

		//Close the transaction Log File
		logWriter.close();
		//exec.shutdownNow();
		System.out.println("Closing Print Writer...");
		//PRINT STATS
		long totalTxns = stats.txnsPerSec();
		System.out.println("Total Number of Txns ran: \t\t"         + totalTxns);
		System.out.println("Estimated Throughput (tps): \t\t"       + totalTxns / (TIMETORUN * 60.0) + " tps");
        System.out.println("Total Writes: \t\t"                     + stats.totalWriteOps());
		System.out.println("Total Number of Operations ran: \t\t"   + stats.totalOps());
		System.out.println("Estimated Throughput (ops): \t\t"       + stats.totalOps() / (TIMETORUN * 60.0) + " ops");

		System.out.println("************************************************************\n\n\n");

		System.out.println("*********************Test Run statistics********************");
		System.out.println("************************************************************");
		System.out.println("Txn Mix:");
		System.out.println("BrokerVolume Txn was run: \t\t"+    stats.txnMix[0] + " times;");
		System.out.println("CustomerPosition Txn was run: \t"+  stats.txnMix[1] + " times;");
		System.out.println("MarketFeed Txn was run: \t\t\t"+    stats.txnMix[2] + " times;");
		System.out.println("TradeOrder Txn was run: \t\t"+      stats.txnMix[3] + " times;");
		System.out.println("TradeResult Txn was run: \t\t"+     stats.txnMix[4] + " times;");
		System.out.println("TradeStatus Txn was run: \t\t"+     stats.txnMix[5] + " times;");
		System.out.println("SecurityDetail Txn was run: \t\t"+  stats.txnMix[6] + " times;");

		System.out.println("\n\n****************** Txn Duration (in msec) ******************");
		System.out.println("************************************************************");
		System.out.println("Broker Volume avg time\t\t: "   + ((double) stats.txnMix[7])/ stats.txnMix[0]);
		System.out.println("Customer Position avg time\t: " + ((double) stats.txnMix[8])/ stats.txnMix[1]);
		System.out.println("Market Feed avg time\t\t: "     + ((double) stats.txnMix[9])/ stats.txnMix[2]);
		System.out.println("Trade Order avg time\t\t: "     + ((double) stats.txnMix[10])/stats.txnMix[3]);
		System.out.println("Trade Result avg rime\t\t: "    + ((double) stats.txnMix[11])/stats.txnMix[4]);
		System.out.println("Trade Status avg rime\t\t: "    + ((double) stats.txnMix[12])/stats.txnMix[5]);
		System.out.println("Security Detail avg rime\t\t: " + ((double) stats.txnMix[13])/stats.txnMix[6]);

	}
	// TODO: THIS METHOD MUST RUN ON ALL(!) DATABASE INSTANCES
	// PRIMARY AND ALL EXTENSION DBs

	public static void initParams() {
		//all_brokers
		//all_brokers = dbObject.QUERY("select b_name from broker");
		//all_sectors
		//all_sectors = dbObject.QUERY("select sc_name from sector");
		//all_customers
		//all_customers = dbObject.QUERY("select c_id from customer");
		//all_symbols
		//all_symbols = dbObject.QUERY("select s_symb from security");
		//account_id
		//all_acct_ids = dbObject.QUERY("select ca_id from customer_account");

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

	static List  DeseriaizeThings(String filename, List javaObject){
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
				for (int i=0; i<20;i++){
					pool.add("BrokerVolume");
				}
				for (int i=20; i<35;i++){
					pool.add("CustomerPosition");
				}
				for (int i=35;i<67;i++){
					pool.add("TradeStatus");
				}
				for (int i=67;i<90;i++){
					pool.add("SecurityDetail");
				}
				for (int i=90;i<100;i++){
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
    */

	public static List<Double> getStats(Vector<Long> list) {
		//returning List contains:
		//min, max, avg
		List results = new Vector<Double>(3);
		double sum = 0.0;
		double min = list.get(0);
		double max = list.get(0);
		for (int i=0; i<list.size(); i++){
			long elem = list.get(i);
			sum += elem;
			if (elem < min){
				min = elem;
			}
			if (elem > max){
				max = elem;
			}
		}
		results.add(min);
		results.add(max);
		results.add(sum/list.size());
		System.out.println(results);
		return results;
	}



	private static void generateTxn(arisDemo d, String txnFrame, Statistics stats) {
		switch (txnFrame)
		{
			case "BrokerVolume":
				//System.out.print("Executing Broker Volume from: ");
				//System.out.println(Thread.currentThread());
				brokerVolumeFrame(d, stats);
				//txnMix[0]++;
				break;
			case "CustomerPosition":
				//System.out.print("Executing Customer Position from: ");
				//System.out.println(Thread.currentThread());
				customerPositionFrame(d, stats);
				break;
			case "MarketFeed":
				//System.out.print("Executing Market Feed from: ");
				//System.out.println(Thread.currentThread());
				marketFeedFrame(d, stats);
				break;
			case "TradeOrder":
				//System.out.print("Executing Trade Order followed by Trade Result from: ");
				//System.out.println(Thread.currentThread());
				tradeOrder(d, stats);
				//stats.increment(3);
				break;
			case "TradeStatus":
				//System.out.print("Executing Trade Status from: ");
				//System.out.println(Thread.currentThread());
				tradeStatus(d, stats);
				//stats.increment(5);
				break;
			case "SecurityDetail":
				securityDetail(d, stats);
				break;
			default:
				System.out.println("Strange Transaction Type Received");
				break;
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
		int number_of_brokers = ThreadLocalRandom.current().nextInt(all_brokers.size());
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
		t = System.currentTimeMillis();
		//dbObject.QUERY(query);
		if (BYPASS){
			shell.executeCommand(query);
		}
		else {
			dbObject.QUERY(query);
		}
		//
		s.insertTime(7, System.currentTimeMillis() - t);
		s.increment(0);
		//s.txnMix[6] = s.txnMix[6] + System.currentTimeMillis() - t;
		//}
	}

	private static void customerPositionFrame(final arisDemo dbObject, Statistics s) {
		Long t = System.currentTimeMillis();
		//Customer Position Frame 1 of 2
		String cust_id = all_customers.get(ThreadLocalRandom.current().nextInt(0, all_customers.size()));
		final String query1 = String.format(
				"SELECT CA_ID, CA_BAL, sum(HS_QTY * LT_PRICE) as soma " +
						"FROM CUSTOMER_ACCOUNT left outer join " +
						"HOLDING_SUMMARY on HS_CA_ID = CA_ID, LAST_TRADE " +
						"WHERE CA_C_ID = '%s' " +
						"GROUP BY CA_ID, CA_BAL " +
						"ORDER BY 3 asc " +
						"LIMIT 10", cust_id);
		//dbObject.QUERY(query1);
		if (BYPASS){
			shell.executeCommand(query1);
		}
		else {
			dbObject.QUERY(query1);
		}

		//Customer Position Frame 2 of 2

		//String c_ad_id = dbObject.QUERY2STR(String.format("select c_ad_id from customer where c_id = '%s'", cust_id));
		String c_ad_id = "4300000189";
		final String q = String.format("select c_ad_id from customer where c_id = '%s'", cust_id);
		if (BYPASS){
			c_ad_id = shell.executeCommand(q).replace("c_ad_id", "").replace("(1 row)", "");
		}
		else{
			try {
				c_ad_id = dbObject.QUERY2MAP(q).get("c_ad_id").toString();
			}catch (NullPointerException e){
				//System.out.println("Null Pointer Exception in CustomerPositionFrame1");
				//System.out.println(dbObject.QUERY(q));
				//System.out.println(dbObject.QUERY2STR(q));
				//System.out.println(dbObject.QUERY2MAP(q));
				//System.out.println(dbObject.QUERY2LST(q));
				//System.out.println(String.format("select c_ad_id from customer where c_id = '%s'", cust_id));
				//return;
			}
		}
		final String query2 = String.format(
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
		//dbObject.QUERY(query2);
		if (BYPASS){
			shell.executeCommand(query2);
		}
		else {
			dbObject.QUERY(query2);
		}
		s.insertTime(8, System.currentTimeMillis() - t);
		s.increment(1);
		//s.txnMix[7] = s.txnMix[7] + System.currentTimeMillis() - t;
	}

	private static void marketFeedFrame(arisDemo dbObject, Statistics s) {
		Long t = System.currentTimeMillis();

		activeSymbols = dbObject.QUERY("select distinct(TR_S_SYMB) from TRADE_REQUEST order by TR_S_SYMB ASC");
		//int numberOfSymbols = ThreadLocalRandom.current().nextInt(0, activeSymbols.size());
		//the previous line is commented out because the spec states that the number of symbols must be 20;
		int numberOfSymbols = 20;
		if (activeSymbols.size() < numberOfSymbols) {numberOfSymbols = activeSymbols.size();}
		if (numberOfSymbols == 0) {return;}
		List activeSymbolsSet = randomSample(activeSymbols, numberOfSymbols);


		//price quote[]
		ArrayList<Double> priceQuote = new ArrayList<Double>(numberOfSymbols);
		for (int i=0; i<numberOfSymbols; i++){
			double low 	= pricesDM.get(activeSymbolsSet.get(i)).get(0);
			double high = pricesDM.get(activeSymbolsSet.get(i)).get(1);
			priceQuote.add(i, ThreadLocalRandom.current().nextDouble(low, high));
		}

		//trade quantity[]
		ArrayList<String> tradeQuantity= new ArrayList<String>(numberOfSymbols);
		for (int i=0; i<numberOfSymbols; i++){
			String tradeQtQuery = String.format("select tr_qty from trade_request where tr_s_symb = '%s' limit 1",
					activeSymbolsSet.get(i));
			try{
				tradeQuantity.add(dbObject.QUERY2MAP(tradeQtQuery).get("tr_qty").toString());
			}catch (Exception e){
				return;
			}
		}
		Long t2;
		dbObject.START_TX();
		for (int i=0; i<numberOfSymbols; i++) {
			t2 = System.currentTimeMillis();
			String query1 = String.format(
					"UPDATE LAST_TRADE " +
							"SET LT_PRICE = %f, " +
							"LT_VOL = LT_VOL + %s, " +
							"LT_DTS = now() " +
							"WHERE LT_S_SYMB = '%s'", priceQuote.get(i), tradeQuantity.get(i), activeSymbolsSet.get(i));
			while (System.currentTimeMillis() < t2 + 15*1000){
			dbObject.DML(query1);

			//store trade_id in request_list
			String query2 = String.format(
					"SELECT TR_T_ID "+
							"FROM TRADE_REQUEST " +
							"WHERE TR_S_SYMB = '%s' and " +
							"((TR_TT_ID = 'TSL' and TR_BID_PRICE >= %.2f) or " +
							"(TR_TT_ID = 'TLS' and TR_BID_PRICE <= %.2f) or " +
							"(TR_TT_ID = 'TLB' and TR_BID_PRICE >= %.2f))",activeSymbolsSet.get(i),priceQuote.get(i),
					priceQuote.get(i),priceQuote.get(i));
			//List<Map<String, Object>> request_list = dbObject.QUERY2LST(query2);
			List<Map<String, Object>> request_list = null;

			request_list = dbObject.QUERY2LST(query2);

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
				break;
			}
			}//end while timeout here
			System.out.println("Market Feed Time out");
			dbObject.DML("update zip_code set zc_div='HL' where zc_code='10001'");
			dbObject.TCL("rollback");
			return;
		}
		dbObject.TCL("commit");
		s.insertTime(9, System.currentTimeMillis() - t);
		s.increment(2);
	}

	//Important:
	//After a successfully Committed market order, the EGenTxnHarness sends the order for the trade to the appropriate MEE.
	//In other words the trade Result txn is called !!!
	private static void tradeOrder(arisDemo dbObject, Statistics s) {
		Long t = System.currentTimeMillis();
		//Frame Inputs: account_id, symbol of the security (stock), trade_quantity
		//bools: type_is_market, type_is_sell, use LIFO or FILO traversal

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

		// Get account, customer, and broker information into a Map
		String  sqlTOF1_1 = String.format(
				"SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st " +
						"FROM customer_account " +
						"WHERE ca_id = %s", acct_id);
		Map output1 = dbObject.QUERY2MAP(sqlTOF1_1);
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
						Map entry = (Map) holdList.get(i);
						if (Integer.parseInt(entry.get("h_qty").toString()) > needed_qty){
							buy_value += needed_qty * Double.parseDouble(entry.get("h_price").toString());
							sell_value += needed_qty * trade_price;
							needed_qty = 0;
							break;
						}
						else {
							buy_value += needed_qty * Double.parseDouble(entry.get("h_price").toString());
							sell_value += needed_qty * trade_price;
							needed_qty = needed_qty - Integer.parseInt(entry.get("h_qty").toString());
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
						if (Integer.parseInt(entry.get("h_qty").toString()) + needed_qty < 0){
							sell_value += needed_qty * Double.parseDouble(entry.get("h_price").toString());
							buy_value += needed_qty * trade_price;
							needed_qty = 0;
							continue;
						}
						else {
							hold_qty = -hold_qty;
							sell_value += hold_qty * Double.parseDouble(entry.get("h_price").toString());
							buy_value += hold_qty * trade_price;
							needed_qty = needed_qty - Integer.parseInt(entry.get("h_qty").toString());
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

		//START TXN
		dbObject.START_TX();
		//:USE EXEC_QUERY
		String trade_id = dbObject.EXEC_QUERY("SELECT NEXTVAL('SEQ_TRADE_ID')");
		String  sqlTOF4_1 = String.format(
				"INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, " +
						"                  t_s_symb, t_qty, t_bid_price, t_ca_id, " +
						"                  t_exec_name, t_trade_price, t_chrg, t_comm, " +
						"                  t_tax, t_lifo) " +
						"VALUES (%s, now(), '%s', '%s', %s, '%s', " +
						"        %d, %s, %s, '%s', NULL, %s, %s, 0, %s) ", trade_id,
				status_id, t_tt_id, t_is_cash, symbol, trade_qty, tradePriceStr, acct_id,
				exec_name, charge_amount, comm_amount, is_lifo);

		dbObject.DML(sqlTOF4_1);


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

		dbObject.TCL("commit");
		s.insertTime(10, System.currentTimeMillis() - t);
		s.increment(3);
		// Invoke tradeResult before exiting method.
		tradeResult(dbObject, s, trade_id, trade_price);
	}

	private static void tradeResult(arisDemo dbObject, Statistics s, String trade_id, double trade_price){
		Long t = System.currentTimeMillis();
		dbObject.START_TX();
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
						Map entry = (Map) holdList.get(i);
						if ( Integer.parseInt(entry.get("h_qty").toString()) > needed_qty){
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
							buy_value += needed_qty * Double.parseDouble(entry.get("h_price").toString());
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

							buy_value += Integer.parseInt(hold_qty) * Double.parseDouble( entry.get("h_price").toString());
							sell_value += Integer.parseInt(hold_qty) * trade_price;
							needed_qty = needed_qty -  Integer.parseInt(entry.get("h_qty").toString());
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
					if (Integer.parseInt(entry.get("h_qty").toString()) + needed_qty < 0) {
						//Bying back some of the short sell
						String trFrame2_4a = String.format(
								"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
										"                            hh_after_qty) " +
										"VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, 0, Integer.parseInt(entry.get("h_qty").toString()) + needed_qty);
						dbObject.DML(trFrame2_4a);
						String trFrame2_5a = String.format(
								"UPDATE holding " +
										"SET h_qty = %d " +
										"WHERE h_t_id = %s", Integer.parseInt(hold_qty)+needed_qty, entry.get("h_t_id"));
						dbObject.DML(trFrame2_5a);
						sell_value 	+= needed_qty*Integer.parseInt(entry.get("h_price").toString());
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
						sell_value += Integer.parseInt(hold_qty) * Integer.parseInt(entry.get("h_price").toString());
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
						"SET ca_bal = ca_bal + (%f) " +
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

		dbObject.TCL("commit");
		s.insertTime(11, System.currentTimeMillis() - t);
		s.increment(4);
		//s.txnMix[10] = s.txnMix[10] + System.currentTimeMillis() - t;
	}

	private static void tradeStatus(final arisDemo dbObject, Statistics s) {
		String acct_id =  all_acct_ids.get(ThreadLocalRandom.current().nextInt(0, all_acct_ids.size())).toString();
		Long t = System.currentTimeMillis();
		final String  sqlTSF1_1 = String.format(
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
		//dbObject.QUERY(sqlTSF1_1);
		if (BYPASS) {
			shell.executeCommand(sqlTSF1_1);
		}
		else {
			dbObject.QUERY(sqlTSF1_1);
		}

		final String  sqlTSF1_2 = String.format(
				"SELECT c_l_name, c_f_name, b_name " +
						"FROM customer_account, customer, broker " +
						"WHERE ca_id = %s " +
						"  AND c_id = ca_c_id " +
						"  AND b_id = ca_b_id", acct_id);
		//dbObject.QUERY(sqlTSF1_2);
		if (BYPASS) {
			shell.executeCommand(sqlTSF1_2);
		}
		else {
			dbObject.QUERY(sqlTSF1_2);
		}
		s.insertTime(12, System.currentTimeMillis() - t);
		s.increment(5);
		//s.txnMix[11] = s.txnMix[11] + System.currentTimeMillis() - t;

	}

	private static void securityDetail(arisDemo dbObject, Statistics s) {
		String symbol = all_symbols.get(ThreadLocalRandom.current().nextInt(0, all_symbols.size()));

		int valRand = ThreadLocalRandom.current().nextInt(5, 21);
		long beginTime;
		long endTime;
		beginTime = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
		endTime = Timestamp.valueOf("2004-12-31 00:58:00").getTime();

		long diff = endTime - beginTime + 1 - valRand;
		Date dateRand = new Date(beginTime + (long) (Math.random() * diff));

		String date = dateRand.toString();
		Long t = System.currentTimeMillis();

		String sdf1_1 = String.format("SELECT s_name," +
				"       co_id," +
				"       co_name," +
				"       co_sp_rate," +
				"       co_ceo," +
				"       co_desc," +
				"       co_open_date," +
				"       co_st_id," +
				"       ca.ad_line1," +
				"       ca.ad_line2," +
				"       zca.zc_town," +
				"       zca.zc_div," +
				"       ca.ad_zc_code," +
				"       ca.ad_ctry," +
				"       s_num_out," +
				"       s_start_date," +
				"       s_exch_date," +
				"       s_pe," +
				"       s_52wk_high," +
				"       s_52wk_high_date," +
				"       s_52wk_low," +
				"       s_52wk_low_date," +
				"       s_dividend," +
				"       s_yield," +
				"       zea.zc_div," +
				"       ea.ad_ctry," +
				"       ea.ad_line1," +
				"       ea.ad_line2," +
				"       zea.zc_town," +
				"       ea.ad_zc_code," +
				"       ex_close," +
				"       ex_desc," +
				"       ex_name," +
				"       ex_num_symb," +
				"       ex_open " +
				"FROM   security," +
				"       company," +
				"       address ca," +
				"       address ea," +
				"       zip_code zca," +
				"       zip_code zea," +
				"       exchange " +
				"WHERE  s_symb = '%s'" +
				"       AND co_id = s_co_id" +
				"       AND ca.ad_id = co_ad_id" +
				"       AND ea.ad_id = ex_ad_id" +
				"       AND ex_id = s_ex_id" +
				"       AND ca.ad_zc_code = zca.zc_code" +
				"       AND ea.ad_zc_code = zea.zc_code", symbol);
		Map values = dbObject.QUERY2MAP(sdf1_1);

		String co_id = values.get("co_id").toString();


		String sdf1_2 = String.format("SELECT co_name, " +
				"       in_name " +
				"FROM   company_competitor, " +
				"       company, " +
				"       industry " +
				"WHERE  cp_co_id = '%s' " +
				"       AND co_id = cp_comp_co_id " +
				"       AND in_id = cp_in_id " +
				"LIMIT %d", co_id, valRand);
		dbObject.QUERY(sdf1_2);

		String sdf1_3 = String.format("SELECT   fi_year," +
				"         fi_qtr," +
				"         fi_qtr_start_date," +
				"         fi_revenue," +
				"         fi_net_earn," +
				"         fi_basic_eps," +
				"         fi_dilut_eps," +
				"         fi_margin," +
				"         fi_inventory," +
				"         fi_assets," +
				"         fi_liability," +
				"         fi_out_basic," +
				"         fi_out_dilut " +
				"FROM     financial " +
				"WHERE    fi_co_id = '%s'" +
				"ORDER BY fi_year ASC," +
				"         fi_qtr " +
				"LIMIT %d", co_id, valRand);
		dbObject.QUERY(sdf1_3);

		String sdf1_4 = String.format("SELECT   dm_date," +
				"         dm_close," +
				"         dm_high," +
				"         dm_low," +
				"         dm_vol " +
				"FROM     daily_market " +
				"WHERE    dm_s_symb = '%s'" +
				"         AND dm_date >= '%s'" +
				"ORDER BY dm_date ASC " +
				"LIMIT %d", symbol, date, valRand);
		dbObject.QUERY(sdf1_4);

		String sdf1_5 = String.format("SELECT lt_price," +
				"       lt_open_price," +
				"       lt_vol " +
				"FROM   last_trade " +
				"WHERE  lt_s_symb = '%s'", symbol);
		dbObject.QUERY(sdf1_5);

		String sdf1_7 = String.format("SELECT " +
				"       ni_dts," +
				"       ni_source," +
				"       ni_author," +
				"       ni_headline," +
				"       ni_summary " +
				"FROM   news_xref," +
				"       news_item " +
				"WHERE  ni_id = nx_ni_id" +
				"       AND nx_co_id = '%s'" +
				"LIMIT %d", co_id, valRand);
		dbObject.QUERY(sdf1_7);

		s.insertTime(13, System.currentTimeMillis() - t);
		s.increment(6);
	}

    /*
	private static void tradeUpdate(arisDemo dbObject) {
		List tradeIdsList = dbObject.QUERY2LST("select t_id from trade");

		Long t = System.currentTimeMillis();

		String SQLTUF1_1 = String.format(
				"SELECT t_exec_name " +
						"FROM trade " +
						"WHERE t_id = %ld", );

		String SQLTUF1_2a = "SELECT REPLACE('%s', ' X ', ' ')";

		String SQLTUF1_2b = "SELECT REPLACE('%s', ' ', ' X ')";

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

	}
	*/

	public class MyRunnable implements Runnable {

		private volatile boolean shutdown;

		public void run(){
			while (!shutdown){
				System.out.println("Running");
			}
		}
		public void shutdown(){
			shutdown = true;
		}
	}

	private static class SimTest implements Callable<String>{
		private Statistics s;
		public arisDemo d;
		SimTest(Statistics stats, arisDemo db){this.s = stats; this.d = db;}

		public void disconnectNow(){
			System.out.println(d.DISCONNECT());
		}

		private volatile boolean cancelled = false;
		public void onCancel() throws IOException{
			cancelled = true;
			d.DISCONNECT();
			Thread.currentThread().interrupt();
		}

		@Override
		//Changed returned type from Object to String
		public String call() throws Exception{
			if (!BYPASS){
				//System.out.println(d.CONNECT());
				d.CONNECT();
				d.setConsistency(MODE);
			}

			//long lStartTime = System.currentTimeMillis();
			int i =0;

			List txnsToRun  = new Vector<String>();
			txnsToRun = workloadMix(MIXSELECTOR);

			//CHECK
			//System.out.println(Thread.currentThread().getName());
			//System.out.println(txnsToRun);
			//END CHECK

			while (!cancelled){
				generateTxn(d, txnsToRun.get(i).toString(), s);
				i++;
				if (i >= txnsToRun.size() - 1) {i = 0;}
			}
			if (Thread.currentThread().isInterrupted()){
				d.DISCONNECT();
			}
			//System.out.println("Thread: " + Thread.currentThread().getName() + " finished");
			return "Session complete";
		}
	}
}
