package hih;

import java.sql.*;
import java.util.List;

/**
 * Created by mariosp on 13/2/16.
 */
public class testTransj {

    static BenStatistics hStats;

    testTransj(BenStatistics stats){this.hStats = stats;}

    private static long nextSeq = 200000000999999L;
    public String getNextSeqNumber(){
        return Long.toString(nextSeq++);
    }

    public void setNextSeq(long next){
        nextSeq=next+1;
        System.out.println("SET SEQUENCE NUMBER to "+nextSeq);
    }

    public void brokerVolumeFrame(Statement st){
        int number_of_brokers = hihUtil.testRndGen.nextInt(hihSerializedData.all_brokers.size()); //ThreadLocalRandom.current()
        // .nextInt(all_brokers.size());
        List active_brokers = hihUtil.randomSample(hihSerializedData.all_brokers, number_of_brokers);
        String sector_name = hihSerializedData.all_sectors.get(hihUtil.testRndGen.nextInt(hihSerializedData.all_sectors.size()));
        String activeBrokersStr = org.apache.commons.lang3.StringUtils.join(active_brokers, ',');
        String query = String.format("SELECT b_name, SUM(tr_qty * tr_bid_price) AS vol " +
                "FROM trade_request, sector, industry, company, broker, security " +
                "WHERE tr_b_id = b_id " +
                "AND tr_s_symb = s_symb " +
                "AND s_co_id = co_id " +
                "AND co_in_id = in_id " +
                "AND sc_id = in_sc_id " +
                "AND b_name = ANY ('{%s}')" +            //'%s' " +//"AND b_name =
                "AND sc_name = '%s' " +
                "GROUP BY b_name " +
                "ORDER BY 2 DESC", activeBrokersStr, sector_name);
        long startTime = System.currentTimeMillis();
        try{
            st.getConnection().setAutoCommit(true);
            PreparedStatement ps = st.getConnection().prepareStatement(query);
            ps.execute();
            hStats.incOperation();
            ps.close();
            ps = null;
        }catch(Exception e){e.printStackTrace();return;}
        long endTime = System.currentTimeMillis();
        hStats.increment(0);
        hStats.insertTime(0, endTime - startTime);
    }

    public void customerPositionFrame(Statement st) {
        ResultSet rs = null;
        //Customer Position Frame 1 of 2
        String cust_id = hihSerializedData.all_customers.get(hihUtil.testRndGen.nextInt(hihSerializedData.all_customers.size()));
        String query1 = String.format(
                "SELECT CA_ID, CA_BAL, sum(HS_QTY * LT_PRICE) as soma " +
                        "FROM CUSTOMER_ACCOUNT left outer join " +
                        "HOLDING_SUMMARY on HS_CA_ID = CA_ID, LAST_TRADE " +
                        "WHERE CA_C_ID = '%s' " +
                        "GROUP BY CA_ID, CA_BAL " +
                        "ORDER BY 3 asc " +
                        "LIMIT 10", cust_id);
        //Customer Position Frame 2 of 2
        String query2a = String.format("select c_ad_id from customer where c_id = '%s'", cust_id);
        String query2b =
                "SELECT T_ID, T_S_SYMB, T_QTY, ST_NAME, TH_DTS " +
                        "FROM (SELECT T_ID as ID " +
                        "FROM TRADE " +
                        "WHERE T_CA_ID = ? " +
                        "ORDER BY T_DTS desc LIMIT 10) as T, " +
                        "TRADE, TRADE_HISTORY, STATUS_TYPE " +
                        "WHERE T_ID = ID " +
                        "AND TH_T_ID = T_ID " +
                        "AND ST_ID = TH_ST_ID " +
                        "ORDER BY TH_DTS desc " +
                        "LIMIT 30";

        long startTime = System.currentTimeMillis();
        try{
            st.getConnection().setAutoCommit(true);
            PreparedStatement ps1 = st.getConnection().prepareStatement(query1);
            PreparedStatement ps2a = st.getConnection().prepareStatement(query2a);
            PreparedStatement ps2b = st.getConnection().prepareStatement(query2b);

            ps1.execute();
            hStats.incOperation();
            rs = ps2a.executeQuery();
            hStats.incOperation();

            Long s = 0L;
            if (rs.next()){
                s = rs.getLong("c_ad_id");
            }
            //ps2b.setLong(1, s);
            //ps2b.execute();
            //hStats.incOperation();

            rs.close(); rs=null;
            ps1.close(); ps1 = null;
            ps2a.close(); ps2a = null;
            ps2b.close(); ps2b = null;

        }catch(Exception e){
            e.printStackTrace();
            try{rs.close();}catch(SQLException se){se.printStackTrace();}
            return;
        }
        long endTime = System.currentTimeMillis();
        hStats.insertTime(1, endTime - startTime);
        hStats.increment(1);
    }
    public void tradeStatus(Statement st) {
        ResultSet rs= null;
        String acct_id =  hihSerializedData.all_acct_ids.get(hihUtil.testRndGen.nextInt(hihSerializedData.all_acct_ids.size()))
                .toString();

        String  sqlTSF1_1 = String.format(
                "SELECT t_id, t_dts, st_name, tt_name, t_s_symb, t_qty, " +
                        "       t_exec_name, t_chrg, s_name, ex_name " +
                        "FROM trade, status_type, trade_type, security, exchange " +
                        "WHERE t_ca_id = %s " +
                        "  AND st_id = t_st_id " +
                        "  AND tt_id = t_tt_id " +
                        "  AND s_symb = t_s_symb " +
                        "  AND ex_id = s_ex_id " +
                        "ORDER BY t_dts DESC " +
                        "LIMIT 50", acct_id);

        String  sqlTSF1_2 = String.format(
                "SELECT c_l_name, c_f_name, b_name " +
                        "FROM customer_account, customer, broker " +
                        "WHERE ca_id = %s " +
                        "  AND c_id = ca_c_id " +
                        "  AND b_id = ca_b_id", acct_id);
        long startTime = System.currentTimeMillis();
        try{
            st.getConnection().setAutoCommit(true);
            Statement s1 = st.getConnection().createStatement();
            rs = s1.executeQuery(sqlTSF1_1);

            hStats.incOperation();
            rs=s1.executeQuery(sqlTSF1_2);

            hStats.incOperation();
            s1.close();
            s1 = null;
        }
        catch(Exception e){
            e.printStackTrace();
            return;
        }
        long endTime = System.currentTimeMillis();
        hStats.insertTime(5, endTime - startTime);
        hStats.increment(5);
    }

    public void securityDetail(Statement st) {
        String symbol = hihSerializedData.all_symbols.get(hihUtil.testRndGen.nextInt(hihSerializedData.all_symbols.size()));
        int valRand = 5 + hihUtil.testRndGen.nextInt(21 - 5);
        long beginTime;
        long endTime;
        beginTime = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
        endTime = Timestamp.valueOf("2004-12-31 00:58:00").getTime();

        long diff = endTime - beginTime + 1 - valRand;
        java.util.Date dateRand = new java.util.Date(beginTime + (long) (Math.random() * diff));

        String date = dateRand.toString();
        Long t = System.currentTimeMillis();

        String sdf1_1 = "SELECT s_name," +
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
                "WHERE  s_symb = ? " +
                "       AND co_id = s_co_id" +
                "       AND ca.ad_id = co_ad_id" +
                "       AND ea.ad_id = ex_ad_id" +
                "       AND ex_id = s_ex_id" +
                "       AND ca.ad_zc_code = zca.zc_code" +
                "       AND ea.ad_zc_code = zea.zc_code";
        //, symbol);
        //Map values = dbObject.QUERY2MAP(sdf1_1);

        //String co_id = values.get("co_id").toString();


        String sdf1_2 = "SELECT co_name, " +
                "       in_name " +
                "FROM   company_competitor, " +
                "       company, " +
                "       industry " +
                "WHERE  cp_co_id = ? " +
                "       AND co_id = cp_comp_co_id " +
                "       AND in_id = cp_in_id " +
                "LIMIT ?"; //, co_id, valRand);
        //dbObject.QUERY(sdf1_2);

        String sdf1_3 = "SELECT   fi_year," +
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
                "WHERE    fi_co_id = ? " +
                "ORDER BY fi_year ASC," +
                "         fi_qtr " +
                "LIMIT ?";
        //, co_id, valRand);
        //dbObject.QUERY(sdf1_3);

        String sdf1_4 = "SELECT   dm_date," +
                "         dm_close," +
                "         dm_high," +
                "         dm_low," +
                "         dm_vol " +
                "FROM     daily_market " +
                "WHERE    dm_s_symb = ?" +
                "         AND dm_date >= ? " +
                "ORDER BY dm_date ASC " +
                "LIMIT ?";
        //, symbol, date, valRand);
        //dbObject.QUERY(sdf1_4);

        String sdf1_5 = "SELECT lt_price," +
                "       lt_open_price," +
                "       lt_vol " +
                "FROM   last_trade " +
                "WHERE  lt_s_symb = ? "; //, symbol);
        //dbObject.QUERY(sdf1_5);

        String sdf1_7 = "SELECT " +
                "       ni_dts," +
                "       ni_source," +
                "       ni_author," +
                "       ni_headline," +
                "       ni_summary " +
                "FROM   news_xref," +
                "       news_item " +
                "WHERE  ni_id = nx_ni_id" +
                "       AND nx_co_id = ? " +
                "LIMIT ?";//, co_id, valRand);
        //dbObject.QUERY(sdf1_7);
        ResultSet rs = null;
        long startTime = System.currentTimeMillis();
        try{
            st.getConnection().setAutoCommit(true);
            PreparedStatement sd1 = st.getConnection().prepareStatement(sdf1_1);
            PreparedStatement sd2 = st.getConnection().prepareStatement(sdf1_2);
            PreparedStatement sd3 = st.getConnection().prepareStatement(sdf1_3);
            PreparedStatement sd4 = st.getConnection().prepareStatement(sdf1_4);
            PreparedStatement sd5 = st.getConnection().prepareStatement(sdf1_5);
            PreparedStatement sd7 = st.getConnection().prepareStatement(sdf1_7);

            sd1.setString(1, symbol);
            rs = sd1.executeQuery(); hStats.incOperation();
            if (rs.next()){
                Long co_id = rs.getLong("co_id");
                sd2.setLong(1, co_id);
                sd3.setLong(1, co_id);
                sd7.setLong(1, co_id);
            }
            sd2.setInt(2, valRand);
            sd3.setInt(2, valRand);

            //sd2.execute(); hStats.incOperation();
            //sd3.execute(); hStats.incOperation();

            //sd4.setString(1, symbol);
            //sd4.setString(2, date);
            //java.sql.Date sqlDate = new java.sql.Date(dateRand.getTime());
            //sd4.setDate(2, sqlDate);
            //sd4.setInt(3, valRand);
            //sd4.execute(); hStats.incOperation();

            //sd5.setString(1, symbol);
            //sd5.execute(); hStats.incOperation();
            //sd7.setInt(2, valRand);
            //sd7.execute(); hStats.incOperation();

            sd1.close(); sd1 = null;
            sd2.close(); sd2 = null;
            sd3.close(); sd3 = null;
            sd4.close(); sd4 = null;
            sd5.close(); sd5 = null;
            sd7.close(); sd7 = null;
            rs.close(); rs = null;
        }
        catch(Exception e){
            e.printStackTrace();
            try{rs.close();}catch(SQLException se){se.printStackTrace();}
            return;
        }
        long endTimer = System.currentTimeMillis();
        hStats.insertTime(6, endTimer-startTime);
        hStats.increment(6);
    }
    //////
}
