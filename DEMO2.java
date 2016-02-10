package hih;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DEMO2 {

	public DEMO2() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) 
	{
		//TLog tlog = new TLog("log.txt");
		int SESSIONS = 8;
		try
		{
			 ExecutorService pool = Executors.newFixedThreadPool(SESSIONS);
			 List<Future<String>> list = new ArrayList<Future<String>>();
			
			  Callable<String> callable = new  HiHClient();
			  //Callable<String> callable = new  TestHihooiDriver();
		      for(int i=0; i< SESSIONS; i++)
		      {
		            //submit Callable tasks to be executed by thread pool
		            Future<String> future = pool.submit(callable);
		            //add Future to the list, we can get return value using Future
		            list.add(future);
		      }
		      for(Future<String> fut : list)
		      {
		    	  	try 
		    	  	{
		               System.out.println("Session response "+fut.get());
		          	} 
		    	  	catch(Exception e)
		  			{
		  				e.printStackTrace();
		  			}
		        }
		      
			pool.shutdownNow();
			while (!pool.isTerminated()) 
			{
				//this can throw InterruptedException, you'll need to decide how to deal with that.
				pool.awaitTermination(1,TimeUnit.MILLISECONDS); 
			}
			//tlog.closeLog();
			//System.out.println(" COMPLETED ..");
		
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
