package hih;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;

public class HiHClient implements Callable 
{
	
		private String LISTENER="dicl09.cut.ac.cy";  //home
	    private long id=0;
	    private int tid=0;
		private HiHListenerClient hih = new HiHListenerClient();
	
		//TLog tlog  = null;
		
		
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
		//######################################################################################################################
		public String DISCONNECT()
		{
			return hih.disconnect();  //Returns "Disconnect Successfuly"
		}
		//######################################################################################################################
			public String EXEC_QUERY(String SQL)	
			{	

					List<Map<String, Object>> rows = hih.executeQuery(SQL);

					String output ="";

					for( int i = rows.size() -1; i >= 0 ; i --)
					{
							Map<String,Object> entry = rows.get(i);
							for (String key : entry.keySet()) 
							{
								output = ""+entry.get(key);
							} 

					}
					return output;

			}
		//######################################################################################################################
		public void QUERY(String SQL)
		{
			
			List<Map<String, Object>> rows = hih.executeQuery(SQL);
			
			String output ="";
			
			for( int i = rows.size() -1; i >= 0 ; i --)
			{
				output += "-----------------------------------------------------------------------\n" ; 

				Map<String,Object> entry = rows.get(i);
			    for (String key : entry.keySet()) 
			    {
			    	output += "|"+entry.get(key)+"|";
			    } 
			    output += "\n-----------------------------------------------------------------------\n" ;  
			}
			System.out.println(output);
		}
		//######################################################################################################################
		public String DML(String SQL)
		{
			return hih.executeUpdate(SQL);
		}
		//######################################################################################################################
		public String FASTLOAD(Vector<String> STMT)
		{
			return hih.fastload(STMT,null);
		}
		//######################################################################################################################
		public String START_TX()
		{
			return hih.startTransaction();
		}
		//######################################################################################################################
		public String SET(String set_cmd)
		{
			return hih.set(set_cmd);
		}
		//######################################################################################################################
		public String TCL(String tcl_cmd)
		{
			if (tcl_cmd.equalsIgnoreCase("commit"))
			{
				return hih.commitTransaction();
			}
			else
			{
				return hih.rollbackTransaction();
			}
		}
		//######################################################################################################################
		public String getUniqueStrings(int size)
		{
			 UUID id = UUID.randomUUID();
			 String a = ""+id;
			 //System.out.println(a.length());
			 if (size > 0)
			 {
				 if (size <= 36)
				 {
					 a = a.substring(0,size);
				 }
			 }
			 return a;
		}
		//######################################################################################################################
		/*public HiHClient(TLog tlog)
		{
			tlog = tlog;
		}*/
		//######################################################################################################################
		public HiHClient() 
		{

		}
		//######################################################################################################################
		@Override
		public Object call() throws Exception 
		{
			
			
			HiHClient d = new HiHClient();
			String session_id = d.CONNECT();
			System.out.println(" SESSION :"+session_id+" STARTED.");
			long lStartTime = System.currentTimeMillis();
			
			System.out.println(d.SET("set consistency level 1"));
			//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			
				
			
			
			
					
					/*
					d.QUERY("select count(*) from account_permission"); 
					d.QUERY("select count(*) from address");             
					d.QUERY("select count(*) from broker");              
					d.QUERY("select count(*) from cash_transaction");    
					d.QUERY("select count(*) from charge");              
					d.QUERY("select count(*) from commission_rate");     
					d.QUERY("select count(*) from company");             
					d.QUERY("select count(*) from company_competitor");  
					d.QUERY("select count(*) from customer");            
					d.QUERY("select count(*) from customer_account");    
					d.QUERY("select count(*) from customer_taxrate");    
					d.QUERY("select count(*) from daily_market");        
					d.QUERY("select count(*) from exchange");            
					d.QUERY("select count(*) from financial");           
					d.QUERY("select count(*) from holding");             
					d.QUERY("select count(*) from holding_history");     
					d.QUERY("select count(*) from holding_summary");     
					d.QUERY("select count(*) from industry");            
					d.QUERY("select count(*) from last_trade");      
					d.QUERY("Select count(*) from users");	
					*/
			//d.DML("delete from users");
			/*
			for (int i=0; i<1000; i++)
			{
					String username = getUniqueStrings(30);
					String password = getUniqueStrings(8);	
					String  dml1 = "insert into users(username,password) values ('"+username+"','"+password+"')";
					String a = d.DML(dml1);
					System.out.println(a);
					
			}
			*/
			//d.QUERY("Select count(*) from users");
			for (int j=0; j<100; j++)
			{
				System.out.println(d.START_TX());
				for (int i=0; i<10; i++)
				{
					String username = getUniqueStrings(30);
					String password = getUniqueStrings(8);	
					String  dml1 = "insert into users(username,password) values ('"+username+"','"+password+"')";
					String a = d.DML(dml1);
					System.out.println(a);
					
				}
				System.out.println(d.TCL("commit"));
				d.QUERY("Select count(*) from users");
			}
			
			
					/*
					username = getUniqueStrings(30);
					password = getUniqueStrings(8);				
					dml1 = "insert into users(username,password) values ('"+username+"','"+password+"')";
					String b = d.DML(dml1);
					System.out.println(b);
					d.QUERY("Select count(*) from users");
					*/
				/*
				System.out.println(d.START_TX());
				//---------------------------------------------------------------
				for (int i=0; i<10; i++)
				{	
					username = getUniqueStrings(30);
					password = getUniqueStrings(8);			
					dml1 = "insert into users(username,password) values ('"+username+"','"+password+"')";
					a = d.DML(dml1);
					System.out.println(a);
					
					d.QUERY("Select count(*) from users");
					d.QUERY("select count(*) from account_permission"); 
					d.QUERY("select count(*) from address");             
					d.QUERY("select count(*) from broker");            
					
				}		
				//---------------------------------------------------------------
				System.out.println(d.TCL("commit"));
				*/
			
			
			//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			///========================================================================================================================
			// Single Read worklod
			/*
			for (int i=0; i<50; i++)
			{
				System.out.println(i);
				d.QUERY("Select count(*) from employee");
				d.QUERY("Select count(*) from users");
				d.QUERY("select count(*) from myids");
			}
			*/
			///========================================================================================================================
			// Single Write workload  1 kai 2
			/*
			for (int i=0; i<50; i++)
			{
				String username = getUniqueStrings(30);
				String password = getUniqueStrings(8);
				//--------------------------------------------------------------------------------------------------
				String  dml1 = "insert into users(username,password) values ('"+username+"','"+password+"')";				
				String a = d.DML(dml1);
				System.out.println(a);
				//--------------------------------------------------------------------------------------------------
				
				id++;
				String dml2 = "insert into myids(id,username,password) values ("+id+",'"+username+"','"+password+"')";
				String b = d.DML(dml2);
				System.out.println(b);
				
			}
			*/
			///========================================================================================================================
			//Single Write and Read workload
			/*
			for (int i=0; i<10; i++)
			{
				String username = getUniqueStrings(30);
				String password = getUniqueStrings(8);
				//--------------------------------------------------------------------------------------------------
							System.out.println(i);
							String  dml1 = "insert into users(username,password) values ('"+username+"','"+password+"')";				
							String a = d.DML(dml1);
							System.out.println(a);
							//--------------------------------------------------------------------------------------------------
							id++;
							String dml2 = "insert into myids(id,username,password) values ("+id+",'"+username+"','"+password+"')";
							String b = d.DML(dml2);
							System.out.println(b);
							d.QUERY("select count(*) from myids");
			}
			*/	
			
			/////////////////////////////////////////////
			long lEndTime = System.currentTimeMillis();
			long difference = lEndTime - lStartTime;
			d.DISCONNECT();
			return session_id+" completed in "+difference+" ms.";
		}
		//######################################################################################################################

}
