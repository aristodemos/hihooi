package hih;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by mariosp on 13/2/16.
 */
public class testTrans {

    static BenStatistics hStats;
    testTrans(BenStatistics stats){
        this.hStats = stats;
    }

    public void brokerVolumeFrame(hihUtil util) {
        int number_of_brokers = hihUtil.testRndGen.nextInt(hihSerializedData.all_brokers.size()); //ThreadLocalRandom
        // .current().nextInt
        // (all_brokers.size());
        List active_brokers = hihUtil.randomSample(hihSerializedData.all_brokers, number_of_brokers);
        String sector_name = hihSerializedData.all_sectors.get(hihUtil.testRndGen.nextInt(hihSerializedData
                .all_sectors.size()));
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
                "ORDER BY 2 DESC", activeBrokersStr, sector_name);  //actoive.brokers.get(i)
        long startTime = System.currentTimeMillis();
        util.hih.OnlineExecuteQuery(query);
        long endTime = System.currentTimeMillis();
        hStats.increment(0);
        hStats.insertTime(0, endTime - startTime);
    }

    public void customerPositionFrame(hihUtil util) {
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
                        "WHERE T_CA_ID = %1$d " +
                        "ORDER BY T_DTS desc LIMIT 10) as T, " +
                        "TRADE, TRADE_HISTORY, STATUS_TYPE " +
                        "WHERE T_ID = ID " +
                        "AND TH_T_ID = T_ID " +
                        "AND ST_ID = TH_ST_ID " +
                        "ORDER BY TH_DTS desc " +
                        "LIMIT 30";

        long startTime = System.currentTimeMillis();
        try{
            util.hih.OnlineExecuteQuery(query1);
            util.hih.OnlineExecuteQuery(query2a);
        }catch(Exception e){
            //e.printStackTrace();
            return;}
        long endTime = System.currentTimeMillis();
        hStats.insertTime(1, endTime - startTime);
        hStats.increment(1);
    }

    public void tradeStatus(hihUtil util) {

        String acct_id =  hihSerializedData.all_acct_ids.get(util.testRndGen.nextInt(hihSerializedData.all_acct_ids.size()))
                .toString();

        String  sqlTSF1_1 = String.format(
                "SELECT t_id, t_dts, st_name, tt_name, t_s_symb, t_qty, " +
                        "t_exec_name, t_chrg, s_name, ex_name " +
                        "FROM trade, status_type, trade_type, security, exchange " +
                        "WHERE t_ca_id=%s " +
                        "  AND st_id=t_st_id " +
                        "  AND tt_id=t_tt_id " +
                        "  AND s_symb=t_s_symb " +
                        "  AND ex_id=s_ex_id " +
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
            util.hih.OnlineExecuteQuery(sqlTSF1_1);
            util.hih.OnlineExecuteQuery(sqlTSF1_2);
        }catch(Exception e){
            //e.printStackTrace();
            return;}
        long endTime = System.currentTimeMillis();
        hStats.insertTime(5, endTime-startTime);
        hStats.increment(5);
    }

    public  void securityDetail(hihUtil util) {
        String symbol = hihSerializedData.all_symbols.get(util.testRndGen.nextInt(hihSerializedData.all_symbols.size()));
        int valRand = 5 + util.testRndGen.nextInt(21 - 5);
        long beginTime;
        long endTime;
        beginTime = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
        endTime = Timestamp.valueOf("2004-12-31 00:58:00").getTime();

        long diff = endTime - beginTime + 1 - valRand;
        java.util.Date dateRand = new java.util.Date(beginTime + (long) (Math.random() * diff));

        String date = dateRand.toString();

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
                "WHERE  s_symb = '%s' " +
                "       AND co_id = s_co_id" +
                "       AND ca.ad_id = co_ad_id" +
                "       AND ea.ad_id = ex_ad_id" +
                "       AND ex_id = s_ex_id" +
                "       AND ca.ad_zc_code = zca.zc_code" +
                "       AND ea.ad_zc_code = zea.zc_code", symbol);




        String sdf1_2 = "SELECT co_name, " +
                "       in_name " +
                "FROM   company_competitor, " +
                "       company, " +
                "       industry " +
                "WHERE  cp_co_id = %1$s " +
                "       AND co_id = cp_comp_co_id " +
                "       AND in_id = cp_in_id " +
                "LIMIT %2$d";//, co_id, valRand);

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
                "WHERE    fi_co_id = %1$s " +
                "ORDER BY fi_year ASC," +
                "         fi_qtr " +
                "LIMIT %2$d";//, co_id, valRand);

        String sdf1_4 = String.format("SELECT   dm_date," +
                "         dm_close," +
                "         dm_high," +
                "         dm_low," +
                "         dm_vol " +
                "FROM     daily_market " +
                "WHERE    dm_s_symb = '%s'" +
                "         AND dm_date >= '%s' " +
                "ORDER BY dm_date ASC " +
                "LIMIT %d", symbol, date, valRand);


        String sdf1_5 = String.format("SELECT lt_price," +
                "       lt_open_price," +
                "       lt_vol " +
                "FROM   last_trade " +
                "WHERE  lt_s_symb = '%s' ", symbol);


        String sdf1_7 = "SELECT " +
                "       ni_dts," +
                "       ni_source," +
                "       ni_author," +
                "       ni_headline," +
                "       ni_summary " +
                "FROM   news_xref," +
                "       news_item " +
                "WHERE  ni_id = nx_ni_id" +
                "       AND nx_co_id = %1$s " +
                "LIMIT %2$d ";//, co_id, valRand);
        //dbObject.QUERY(sdf1_7);

        long startTime = System.currentTimeMillis();
        try{
            util.hih.OnlineExecuteQuery(sdf1_1);
            //Map values = util.QUERY2MAP(sdf1_1);
            //String co_id = values.get("co_id").toString();
            //util.QUERY(String.format(sdf1_2, co_id, valRand));
            //util.QUERY(String.format(sdf1_3, co_id, valRand));
            //util.QUERY(sdf1_4);
            //util.QUERY(sdf1_5);
            //util.QUERY(String.format(sdf1_7, co_id, valRand));
        }catch(Exception e){
            //e.printStackTrace();
            return;}
        long endTimer = System.currentTimeMillis();
        hStats.insertTime(6, endTimer-startTime);
        hStats.increment(6);
    }
    ////////////END CLASS////////////
}
