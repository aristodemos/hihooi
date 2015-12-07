package hih;

/**
 * Created by mariosp on 5/12/15.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;


public class bypassHihooi {

    public static void main(String[] args){
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

        String url = "jdbc:postgresql://localhost/dbt5";
        String user = "mariosp";
        String password = "";

        try{
            con = DriverManager.getConnection(url, user, password);
            st  = con.createStatement();
            rs  = st.executeQuery("SELECT max(t_id) from trade");

            if (rs.next()){
                System.out.println(rs.getString(1));
            }
        }
        catch(SQLException sqle) {sqle.printStackTrace();}
        finally{
            try {
                rs.close();
                st.close();
                con.close();
            } catch (SQLException sqlex){sqlex.printStackTrace();}
        }

    }//end main
}//close Class bypassHihooi