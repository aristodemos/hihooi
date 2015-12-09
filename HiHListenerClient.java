package hih;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
 
public class HiHListenerClient
{
	public HiHListenerClient()
	{
		
	}
	
	private PrintWriter out;
	private BufferedReader in;
	private boolean CONNECTED=false;
	private String SESSION_ID="";
	private String CONNECTION_TYPE="USER";
	private String SERVICE_NAME="TESTSRV";
	
	//private List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
	
	 //####################################################################################################################################################
	   //####################################################################################################################################################
		public String rollbackTransaction()
		{
			String exit_code="Rollback Failed.";
			String serverResponse="";
			if (!CONNECTED)
	        {
				exit_code=" You are no connected.";
	        }
			else
			{
				
				String xml_commit =""+ 
	         			"<?xml version=\"1.0\"?>" + 
	     				"<root>" +
	     				"<RequestType>Rollback</RequestType>" + 
     				"<tcl>" +
      				"	<service-name>"+this.SERVICE_NAME+"</service-name>" + 
      				"	<session-id>"+this.SESSION_ID+"</session-id>" + 
       				"</tcl>"+ 
	     				"</root>";
				
				try
				{
						out.println(xml_commit);
						serverResponse=in.readLine();
						return serverResponse;
				}
				catch(Exception e)
				{
					exit_code=e.getMessage();
				}
			}
			
			return exit_code;
		}
	
	   //####################################################################################################################################################
	   //####################################################################################################################################################
		public String commitTransaction()
		{
			String exit_code="Commit Failed.";
			String serverResponse="";
			if (!CONNECTED)
	        {
				exit_code=" You are no connected.";
	        }
			else
			{
				
				String xml_commit =""+ 
	         			"<?xml version=\"1.0\"?>" + 
	     				"<root>" +
	     				"<RequestType>Commit</RequestType>" + 
        				"<tcl>" +
         				"	<service-name>"+this.SERVICE_NAME+"</service-name>" + 
         				"	<session-id>"+this.SESSION_ID+"</session-id>" + 
          				"</tcl>"+ 
	     				"</root>";
				
				try
				{
						out.println(xml_commit);
						serverResponse=in.readLine();
						return serverResponse;
				}
				catch(Exception e)
				{
					exit_code=e.getMessage();
				}
			}
			
			return exit_code;
		}
	   //####################################################################################################################################################
	   //####################################################################################################################################################
		public String executeUpdate(String sql_stmt)
		{
			/*
			 *  0  no insert .
			 *  > 1 OK
			 */
			String exit_code="";
			String serverResponse="";
			if (!CONNECTED)
	        {
				exit_code="You are no connected.";
	        }
			else
			{
				
				String xml_execute_update =""+ 
	         			"<?xml version=\"1.0\"?>" + 
	     				"<root>" +
	     				"<RequestType>Execute Update</RequestType>" + 
        				"<dml>" +
         				"	<service-name>"+this.SERVICE_NAME+"</service-name>" + 
         				"	<session-id>"+this.SESSION_ID+"</session-id>" + 
         				"	<statement>"+XMLEscapeConverter(sql_stmt)+"</statement>" +
          				"</dml>"+ 
	     				"</root>";
				
				try
				{
						out.println(xml_execute_update);
						serverResponse=in.readLine();
						return serverResponse;
				}
				catch(Exception e)
				{
					exit_code=e.getMessage();
				}
			}
			
			return exit_code;
		}
		//####################################################################################################################################################
		//####################################################################################################################################################
				public String set(String input)
				{
					String exit_code="";
					String serverResponse="";
					if (!CONNECTED)
			        {
						exit_code="You are no connected.";
			        }
					else
					{

						try
						{
								out.println(input);
								serverResponse=in.readLine();
								return serverResponse;
						}
						catch(Exception e)
						{
							exit_code=e.getMessage();
						}
					}
					
					return exit_code;
				}
		//####################################################################################################################################################
		//####################################################################################################################################################
		public String startTransaction()
		{
			String exit_code="";
			String serverResponse="";
			if (!CONNECTED)
	        {
				exit_code="You are no connected.";
	        }
			else
			{
				
				String cmd ="Start Transaction";
				
				try
				{
						out.println(cmd);
						serverResponse=in.readLine();
						return serverResponse;
				}
				catch(Exception e)
				{
					exit_code=e.getMessage();
				}
			}
			return exit_code;
		}
	   //####################################################################################################################################################
	   private String XMLEscapeConverter(String input)
	   {
		   input= input.replaceAll("&", "&amp;");
		   input= input.replaceAll("<", "&lt;");
		   input= input.replaceAll(">", "&gt;");
		   input= input.replaceAll("'", "&apos;");
		   //from stack overflow
		   //http://stackoverflow.com/questions/3030903/content-is-not-allowed-in-prolog-when-parsing-perfectly-valid-xml-on-gae
		   //trying to avoid org.xml.sax.SAXParseException;
		   //exception message says: "Content is not allowed in prolog."
		   input= input.trim().replaceFirst("(^([\\W]+)<)","<");
		   return input;
	   }
	   //####################################################################################################################################################
		public List<Map<String, Object>> executeQuery(String query)
		{
			List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
			rows.clear();
			
			String serverResponse="";
			if (!CONNECTED)
	        {
				Map<String, Object> columns = new LinkedHashMap<String, Object>();
				for (int i = 1; i <= 1; i++) 
				{
						columns.put("output", "You are no connected.");
				}
				rows.add(columns);
	        }
			else
			{
				
				String xml_create_session =""+ 
	         			"<?xml version=\"1.0\"?>" + 
	     				"<root>" +
	     				"<RequestType>Execute Query</RequestType>" + 
        				"<query>" +
         				"	<service-name>"+this.SERVICE_NAME+"</service-name>" + 
         				"	<session-id>"+this.SESSION_ID+"</session-id>" + 
         				"	<statement>"+XMLEscapeConverter(query)+"</statement>" +
          				"</query>"+ 
	     				"</root>";
				try
				{
					out.println(xml_create_session);
					serverResponse=in.readLine();
			            if (serverResponse.equalsIgnoreCase("START DATA"))
			            {
			            	int i=0;
			            	while (!serverResponse.equalsIgnoreCase("END DATA"))
			            	{
			            		i++;
			            		serverResponse=in.readLine();
			            		/////System.out.println("serverResponse: "+serverResponse);////
			            		if (!serverResponse.startsWith("END DATA"))
			            		{
			            			Map<String, Object> columns = parseXML(serverResponse,"row");
			            			rows.add(columns);
			            		}
			            	}
			            }
					
				}
				catch(Exception e)
				{
					//System.out.println(new Date()+ e.getMessage());
				}

			}
			return rows;
		}

	//####################################################################################################################################################
	//####################################################################################################################################################
	private Vector getColumnList(String xml_data,String elementTag)
	{
		//System.out.println(xml_data);
		Vector columns = new Vector();
		try
		{
   			DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			InputSource data_source = new InputSource();
			data_source.setCharacterStream(new StringReader(xml_data));			
			Document doc = db.parse(data_source);
			NodeList nodeList = doc.getElementsByTagName(elementTag).item(0).getChildNodes();
			for (int i = 0; i < nodeList.getLength(); i++)
				    switch (nodeList.item(i).getNodeType()) {
				    case Node.ELEMENT_NODE:

				        Element element = (Element) nodeList.item(i);
				        //System.out.println("element name: " + element.getNodeName());
				        columns.add(""+element.getNodeName());			       
				      break;  
				    }
		}
		catch(Exception e)
		{
			//e.printStackTrace();
			columns=null;
		}
		return columns;
	}
	//####################################################################################################################################################
	//####################################################################################################################################################
	private  Map<String, Object>   parseXML(String xml_data,String elementTag)
	{	
	     Vector xmlColumnList = getColumnList(xml_data,elementTag);
	     Map<String, Object> columns = new LinkedHashMap<String, Object>();
	     
	     
	     if ((xmlColumnList!=null)&&(xmlColumnList.size()>0))
	     {
	    	 	for (int i=0; i<xmlColumnList.size(); i++)
	    	 	{
	    	 		String name = (String)xmlColumnList.elementAt(i);
	    	 		//System.out.println(name);
	    	 		try
	    	 		{
	    	 			DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	    	 			InputSource data_source = new InputSource();
		    			data_source.setCharacterStream(new StringReader(xml_data));
		    			Document doc = db.parse(data_source);
		    			NodeList nodes = doc.getElementsByTagName(elementTag);
		    			for (int q = 0; q < nodes.getLength(); q++) 
		    			{
		    			      Element element = (Element) nodes.item(q);
		    			      NodeList identifier = element.getElementsByTagName(name);
		    			      Element line = (Element) identifier.item(0);    
		  				      
		  				     
		  				      for (int k = 1; k <= xmlColumnList.size(); k++) 
		  				      {
		  				    	  columns.put(name,getCharacterDataFromElement(line));
		  				      }
		    			}
	    	 		}
	    	 		catch(Exception e)
	    	 		{
	    	 			System.out.println(e.getMessage());
	    	 		}    	 	
	    	 	
	    	 	}
	     }
	     return columns;
	}
	//####################################################################################################################################################
	//####################################################################################################################################################
	private String getCharacterDataFromElement(Element e) 
	{
	    Node child = e.getFirstChild();
	    if (child instanceof CharacterData) {
	      CharacterData cd = (CharacterData) child;
	      return cd.getData();
	    }
	    return "";
	}
	//####################################################################################################################################################
	//####################################################################################################################################################
	public String connect(Properties p)
	{

        String serverResponse="";
        String exit_code="";
    	String server="";
        try 
        {
        	if (!CONNECTED)
        	{
            		String requestType="Create Session";
            		String username = p.getProperty("username");
            		String password = p.getProperty("password");
            		String identifier = p.getProperty("identifier");
            		String service_name = p.getProperty("service_name");
            		server = p.getProperty("server");
            		String port = p.getProperty("port");
            	
            		Socket socket = new Socket(server, Integer.parseInt(port));
            		this.out = new PrintWriter(socket.getOutputStream(), true);
            		this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            	
            	
            		out.println("Begin");
            		serverResponse= in.readLine();
            	
            		if  (serverResponse.equalsIgnoreCase("Ready"))
            		{
            			String xml_create_session =""+ 
                 			"<?xml version=\"1.0\"?>" + 
             				"<root>" +
             				"<RequestType>"+requestType+"</RequestType>"+
             				"<session>" +
             				"	<service-name>"+this.SERVICE_NAME+"</service-name>" + 
             				"	<username>"+XMLEscapeConverter(username)+"</username>" +
             				"	<password>"+XMLEscapeConverter(password)+"</password>" +
             				"	<identifier>"+XMLEscapeConverter(identifier)+"</identifier>" +
             				"</session>" + 
             				"</root>";
            			out.println(xml_create_session);
            			serverResponse= in.readLine();
            		 	
            			
            			try
            		 	{
            		 		SESSION_ID=serverResponse;
            		 		if (SESSION_ID.equalsIgnoreCase("Connected."))
            		 		{
            		 			exit_code=" * Session is already connected. Disconnect First.";
            		 		}
            		 		else if(SESSION_ID.equalsIgnoreCase("Broken."))
            		 		{
            		 			exit_code=" Session is Broken.";
            		 			disconnect();
            		 		}
            		 		else
            		 		{
            		 			exit_code=SESSION_ID;
            		 			this.CONNECTED=true;
            		 		}
  
            		 	}
            		 	catch(Exception e)
            		 	{
            		 		exit_code=e.getMessage();
            		 		disconnect();
            		 	}
            		}
            		else
            		{
            			exit_code="No Response from server.";
            			disconnect();
            		}
        	}
        	else
        	{
        		exit_code=" Session is already connected. Disconnect First.";
        	}
                 
        } 
		catch (UnknownHostException e) 
		{
			exit_code="Unknown host " + server;
		} 
		catch (IOException e) 
		{
			exit_code=" Couldn't get I/O for the connection to " +server;
		}
		return exit_code;
	}
	
	//####################################################################################################################################################
	//####################################################################################################################################################
	public String disconnect()
	{
		    String exit_code="";
			String serverResponse="";
			if (CONNECTED)
        	{
				try
				{
					out.println("End");
					serverResponse= in.readLine();
					if (serverResponse.equalsIgnoreCase("Bye"))
					{
						this.out.close();
						this.in.close();
						exit_code="Disconnect Successfuly";
					}
					else
					{
						exit_code="No Response from server.";
					}

				}
				catch(Exception e)
				{
					//System.out.println(new Date()+ e.getMessage());
				}
				return exit_code;
        	}
			else
			{
				exit_code="Disconnect Successfuly";
				return exit_code;
			}
				
	}
	//####################################################################################################################################################
	//####################################################################################################################################################
	
	public static void main(String[] args) throws IOException 
    {       
            
		Properties p = new Properties();
		p.setProperty("server","sichari");
		p.setProperty("port","7777");
		
		/*
		p.setProperty("username","user01");
		p.setProperty("password","12345678");
		p.setProperty("identifier","client1");
		p.setProperty("service_name","TESTSRV");
		*/
		
		p.setProperty("username","sys");
		p.setProperty("password","sys");
		p.setProperty("identifier","sys");
		p.setProperty("service_name","TESTSRV");
		
		HiHListenerClient hih = new HiHListenerClient();
		System.out.println(hih.connect(p));
		

		//List<Map<String, Object>> rows = hih.getActiveSession();
		List<Map<String, Object>> rows = hih.executeQuery("select * from hih_mem_sessions");
		
		for( int i = rows.size() -1; i >= 0 ; i --)
		{
			Map<String,Object> entry = rows.get(i);
		    for (String key : entry.keySet()) 
		    {
		    	System.out.println(key);
		    	System.out.println(entry.get(key));		    	
		    } 
		   
		}
		
		System.out.println(hih.disconnect());
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
			// Example 1
            //###########################################################################################################
            /*
              String hostName = "sichari";
        	  int portNumber = 7777;
        
    		  String xml_create_session =""+ 
    			"<?xml version=\"1.0\"?>" + 
				"<root>" +
				"<type>Create Session</type>"+
				"<session>" +
				"	<service-name>TESTSRV</service-name>" + 
				"	<username>user01</username>" + 
				"	<password>12345678</password>" + 
				"	<identifier>Client1</identifier>" + 
				"</session>" + 
				"</root>";
        
    		String numberList = "";
  
              
            
            out.println("Begin");
            System.out.println("Response: " + in.readLine());
            out.println("Fname");
            System.out.println("Response: " + in.readLine());
            for (int i=0; i < 10; i++)
            {
            	out.println("Lname");
            	System.out.println("Response: " + in.readLine());
            }
            out.println(xml_create_session);
            System.out.println("Response: " + in.readLine());
            out.println("End");
            System.out.println("Response: " + in.readLine());
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            */
            //###########################################################################################################
            
            
            //Example 2
            //###########################################################################################################
            /*
            String input="";
            out.println("Begin");
            System.out.println("Response: " + in.readLine());
            
            out.println("Multiplication(10,2)");
            input=in.readLine();
            System.out.println("Response: " + input);
            if (input.equalsIgnoreCase("START DATA"))
            {
            	while (!input.equalsIgnoreCase("END DATA"))
            	{
            		input=in.readLine();
            		System.out.println("Response: " + input);
            	}
            }
            
            out.println("Fname");
            System.out.println("Response: " + in.readLine());
            for (int i=0; i < 10; i++)
            {
            	out.println("Lname");
            	System.out.println("Response: " + in.readLine());
            }
            out.println(xml_create_session);
            System.out.println("Response: " + in.readLine());
            
            
            out.println("Multiplication(10,2)");
            
            input=in.readLine();
            System.out.println("Response: " + input);
            
            if (input.equalsIgnoreCase("START DATA"))
            {
            	while (!input.equalsIgnoreCase("END DATA"))
            	{
            		input=in.readLine();
            		System.out.println("Response: " + input);
            	}
            	out.println("RECEIVED DATA");
            }
            
            out.println("Fname");
            System.out.println("Response: " + in.readLine());
            for (int i=0; i < 10; i++)
            {
            	out.println("Lname");
            	System.out.println("Response: " + in.readLine());
            }
            out.println(xml_create_session);
            System.out.println("Response: " + in.readLine());
            
            out.println("End");
            System.out.println("Response: " + in.readLine());
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            */
            //###########################################################################################################
    }
}

