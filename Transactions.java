package hih;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Created by mariosp on 23/1/16.
 */
public class Transactions {

    static BenStatistics hStats = new BenStatistics();

    public static void brokerVolumeFrame(Statement st){
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
            //rs = st.executeQuery(query);
            /*
            while (rs.next()){
                System.out.print("Bname: " + rs.getString("b_name"));
                System.out.println("Sum: " + rs.getString("vol"));
            }*/
            ps.close();
            ps = null;
        }catch(Exception e){e.printStackTrace();return;}
        long endTime = System.currentTimeMillis();
        hStats.increment(0);
        hStats.insertTime(0, endTime - startTime);
    }

    public static void customerPositionFrame(Statement st) {
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
            ps2b.setLong(1, s);
            ps2b.execute();
            hStats.incOperation();

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

    public static void marketFeedFrame(Statement st) {
        ResultSet rs = null;
        String qSymbSet = "select distinct tr_s_symb, random() from TRADE_REQUEST order by random() limit 20";
        String tradeQtQuery = "select tr_qty from trade_request where tr_s_symb = ? limit 1";
        String qMF1pre = "SELECT * from  LAST_TRADE WHERE LT_S_SYMB = ? FOR UPDATE";
        String qMF1 = "UPDATE LAST_TRADE " +
                "SET LT_PRICE = ?, " +
                "LT_VOL = LT_VOL + ?, " +
                "LT_DTS = now() " +
                "WHERE LT_S_SYMB = ? ";
        //, priceQuote.get(i), tradeQuantity.get(i), activeSymbolsSet.get(i));

        String qMF2 = "SELECT TR_T_ID "+
                "FROM TRADE_REQUEST " +
                "WHERE TR_S_SYMB = ? and " +
                "((TR_TT_ID = 'TSL' and TR_BID_PRICE >= ?) or " +
                "(TR_TT_ID = 'TLS' and TR_BID_PRICE <= ?) or " +
                "(TR_TT_ID = 'TLB' and TR_BID_PRICE >= ?))";
        //,activeSymbolsSet.get(i),priceQuote.get(i),priceQuote.get(i),priceQuote.get(i));

        String qMF3 =   "UPDATE TRADE " + "SET T_DTS = now(), T_ST_ID = 'SBMT' WHERE T_ID = ?";
        //, request_list.get(j).get("tr_t_id"));
        String qMF4 =   "DELETE FROM TRADE_REQUEST WHERE TR_T_ID = ?";
        //, request_list.get(j).get("tr_t_id"));
        String qMF5 =   "INSERT INTO TRADE_HISTORY VALUES (?, now(), 'SBMT')";
        //, request_list.get(j).get("tr_t_id"));


        int numberOfSymbols = 20;
        List<String> activeSymbolSet = new ArrayList();
        //MAFIA:
        //List activeSymbolsSet = helper.randomSample(helper.all_symbols, numberOfSymbols);
        //price quote[]
        long startTime = System.currentTimeMillis();
        try{
            st.getConnection().setAutoCommit(true);
            PreparedStatement psSymbSet = st.getConnection().prepareStatement(qSymbSet);
            PreparedStatement psTrQty   = st.getConnection().prepareStatement(tradeQtQuery);
            //PreparedStatement psMF1pre  = st.getConnection().prepareStatement(qMF1pre);
            PreparedStatement psMF1     = st.getConnection().prepareStatement(qMF1);
            PreparedStatement psMF2     = st.getConnection().prepareStatement(qMF2);
            PreparedStatement psMF3     = st.getConnection().prepareStatement(qMF3);
            PreparedStatement psMF4     = st.getConnection().prepareStatement(qMF4);
            PreparedStatement psMF5     = st.getConnection().prepareStatement(qMF5);

            rs = psSymbSet.executeQuery();
            hStats.incOperation();
            while (rs.next()){
                activeSymbolSet.add(rs.getString("tr_s_symb"));
            }
            if (activeSymbolSet.size()<1){
                return;
            }

            if (activeSymbolSet.size() < numberOfSymbols) {numberOfSymbols = activeSymbolSet.size();}
            activeSymbolSet = hihUtil.randomSample(activeSymbolSet, numberOfSymbols);

            ArrayList<Double> priceQuote = new ArrayList<Double>(numberOfSymbols);

            for (int i=0; i<numberOfSymbols; i++){
                double low 	= hihSerializedData.pricesDM.get(activeSymbolSet.get(i)).get(0);
                double high = hihSerializedData.pricesDM.get(activeSymbolSet.get(i)).get(1);
                double randInt = Math.abs(hihUtil.testRndGen.nextDouble());
                priceQuote.add(i, 0.7*low + randInt*(1.3*high-low) );
            }

            ArrayList<String> tradeQuantity= new ArrayList<String>(numberOfSymbols);

            for (int i=0; i<numberOfSymbols; i++){
                psTrQty.setString(1, activeSymbolSet.get(i));
                rs = psTrQty.executeQuery();
                hStats.incOperation();
                if (rs.next()){
                    tradeQuantity.add(i, rs.getString("tr_qty"));
                }
            }

            for (int i=0; i<numberOfSymbols; i++) {
                st.getConnection().setAutoCommit(false);
                //psMF1pre.setString(1, activeSymbolSet.get(i));

                psMF1.setFloat(1, priceQuote.get(i).floatValue());
                psMF1.setInt(2, Integer.parseInt(tradeQuantity.get(i)));
                psMF1.setString(3, activeSymbolSet.get(i));
                psMF1.executeUpdate();
                hStats.incWriteOp();

                psMF2.setString(1, activeSymbolSet.get(i));
                psMF2.setFloat(2, priceQuote.get(i).floatValue());
                psMF2.setFloat(3, priceQuote.get(i).floatValue());
                psMF2.setFloat(4, priceQuote.get(i).floatValue());
                rs = psMF2.executeQuery();
                hStats.incOperation();

                List<Long> request_list = new ArrayList<>();
                while (rs.next()){
                    //request_list.add(Long.parseLong(rs.getString("TR_T_ID")));
                    request_list.add(rs.getLong("TR_T_ID"));
                }

                for (int j=0; j<request_list.size();j++) {
                    psMF3.setLong(1, request_list.get(j));
                    psMF3.executeUpdate();
                    hStats.incWriteOp();
                    psMF4.setLong(1, request_list.get(j));
                    psMF4.executeUpdate();
                    hStats.incWriteOp();
                    psMF5.setLong(1, request_list.get(j));
                    psMF5.executeUpdate();
                    hStats.incWriteOp();
                }
                //when finished, commit! and set autocommit to TRUE again
                st.getConnection().commit();
            }
            st.getConnection().setAutoCommit(true);
            rs.close();     rs=null;
            psMF1.close();  psMF1=null;
            psMF2.close();  psMF2=null;
            psMF3.close();  psMF3=null;
            psMF4.close();  psMF4=null;
            psMF5.close();  psMF5=null;
        }
        catch(Exception e){
            e.printStackTrace();
            try{
                st.getConnection().rollback();
                st.getConnection().setAutoCommit(true);
                rs.close();
            }catch (SQLException exc){
                exc.printStackTrace();
            }
            return;
        }
        long endTime = System.currentTimeMillis();
        hStats.insertTime(2, endTime-startTime);
        hStats.increment(2);
    }

    public static void tradeStatus(Statement st) {

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
            s1.execute(sqlTSF1_1); hStats.incOperation();
            s1.execute(sqlTSF1_2); hStats.incOperation();
            s1.close();
            s1 = null;
        }
        catch(Exception e){
            e.printStackTrace();
            return;
        }
        long endTime = System.currentTimeMillis();
        hStats.insertTime(5, endTime-startTime);
        hStats.increment(5);
    }

    public static void securityDetail(Statement st) {
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

            sd2.execute(); hStats.incOperation();
            sd3.execute(); hStats.incOperation();

            sd4.setString(1, symbol);
            //sd4.setString(2, date);
            java.sql.Date sqlDate = new java.sql.Date(dateRand.getTime());
            sd4.setDate(2, sqlDate);
            sd4.setInt(3, valRand);
            sd4.execute(); hStats.incOperation();

            sd5.setString(1, symbol);
            sd5.execute(); hStats.incOperation();
            sd7.setInt(2, valRand);
            sd7.execute(); hStats.incOperation();

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

    public static String[] tradeOrder(Statement st) {
        ResultSet rs = null;
        String toResult[] = new String[2];

        String acct_id =  hihSerializedData.all_acct_ids.get(hihUtil.testRndGen.nextInt(hihSerializedData.all_acct_ids.size()))
                .toString();
        String symbol = hihSerializedData.all_symbols.get(hihUtil.testRndGen.nextInt(hihSerializedData.all_symbols.size()));

        int trade_qty = hihUtil.testRndGen.nextInt(800)+100;

        boolean type_is_market 	= hihUtil.testRndGen.nextBoolean();
        boolean type_is_sell	= hihUtil.testRndGen.nextBoolean();

        String t_tt_id = "";
        if (type_is_market){
            if (type_is_sell) {t_tt_id = "TMS";}
            else {t_tt_id = "TMB";}
        }
        else {
            if (type_is_sell) {t_tt_id = "TLS";}
            else {t_tt_id = "TLB";}
        }
        boolean is_lifo		= hihUtil.testRndGen.nextBoolean();
        boolean t_is_cash	= hihUtil.testRndGen.nextBoolean();

        // Get account, customer, and broker information into a Map
        String  sqlTOF1_1 = "SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st " +
                        "FROM customer_account " +
                        "WHERE ca_id = ?"; //acct_id

        String  sqlTOF1_2 = "SELECT c_f_name, c_l_name, c_tier, c_tax_id " +
                        "FROM customer " +
                        "WHERE c_id = ?"; //, output1.get("ca_c_id"));

        String  sqlTOF1_3 = "SELECT b_name " +
                        "FROM Broker  " +
                        "WHERE b_id = ?";
                        //, broker_id);

        String  sqlTOF2_1 = "SELECT ap_acl  " +
                        "FROM account_permission " +
                        "WHERE ap_ca_id = ? " +
                        "  AND ap_f_name = ? " +
                        "  AND ap_l_name = ? " +
                        "  AND ap_tax_id = ? ";
        //, acct_id, output2.get("c_f_name"), output2.get("c_l_name"), output2   .get("c_tax_id"));

        String  sqlTOF3_1b = "SELECT s_co_id, s_ex_id, s_name " +
                        "FROM security " +
                        "WHERE s_symb = ? ";
                        //, symbol);

        String  sqlTOF3_2b = "SELECT co_name " +
                        "FROM company " +
                        "WHERE co_id = ?";
                        //, output5.get("s_co_id"));

        String  sqlTOF3_3 = "SELECT lt_price " +
                        "FROM last_trade " +
                        "WHERE lt_s_symb = ?";
                        //, symbol);

        String  sqlTOF3_5 = "SELECT hs_qty " +
                        "FROM holding_summary " +
                        "WHERE hs_ca_id = ? " +
                        "  AND hs_s_symb = ?";
                        //, acct_id, symbol);

        String  sqlTOF3_6a = "SELECT h_qty, h_price " +
                        "FROM holding " +
                        "WHERE h_ca_id = ? " +
                        "  AND h_s_symb = ? " +
                        "ORDER BY h_dts DESC";
                        //, acct_id, symbol);

        String  sqlTOF3_6b = "SELECT h_qty, h_price " +
                        "FROM holding " +
                        "WHERE h_ca_id = ? " +
                        "  AND h_s_symb = ? " +
                        "ORDER BY h_dts ASC";
                        //, acct_id, symbol);

        long startTime = System.currentTimeMillis();
        try{
            st.getConnection().setAutoCommit(false);

            PreparedStatement psTO1 = st.getConnection().prepareStatement(sqlTOF1_1);
            PreparedStatement psTO1_2 = st.getConnection().prepareStatement(sqlTOF1_2);
            PreparedStatement psTO1_3 = st.getConnection().prepareStatement(sqlTOF1_3);
            PreparedStatement psTO2_1 = st.getConnection().prepareStatement(sqlTOF2_1);
            PreparedStatement psTO3_1b = st.getConnection().prepareStatement(sqlTOF3_1b);
            //
            PreparedStatement psTO3_2b = st.getConnection().prepareStatement(sqlTOF3_2b);
            PreparedStatement psTO3_3 = st.getConnection().prepareStatement(sqlTOF3_3);
            PreparedStatement psTO3_5 = st.getConnection().prepareStatement(sqlTOF3_5);
            PreparedStatement psTO3_6a = st.getConnection().prepareStatement(sqlTOF3_6a);
            PreparedStatement psTO3_6b = st.getConnection().prepareStatement(sqlTOF3_6b);
            //

            psTO1.setLong(1, Long.parseLong(acct_id));
            rs = psTO1.executeQuery(); hStats.incOperation();

            String exec_name    ="";
            long ca_c_id      = 0L;
            String broker_id    ="";
            String c_f_name     ="";
            String c_l_name     ="";
            String c_tax_id     ="";
            long s_co_id      = 0L;
            //
            double hold_price;
            double buy_value = 0.0, sell_value = 0.0;
            int hold_qty = 0, needed_qty = trade_qty;


            if (rs.next()){
                ca_c_id     = rs.getLong("ca_c_id");
                broker_id   = rs.getString("ca_b_id");
                exec_name   = rs.getString("ca_name");
            }

            psTO1_2.setLong(1, ca_c_id);
            rs = psTO1_2.executeQuery(); hStats.incOperation();
            if (rs.next()){
                c_f_name = rs.getString("c_f_name");
                c_l_name = rs.getString("c_l_name");
                c_tax_id = rs.getString("c_tax_id");
            }

            psTO1_3.setLong(1, Long.parseLong(broker_id));
            psTO1_3.execute();
            hStats.incOperation();

            psTO2_1.setLong(1, Long.parseLong(acct_id));
            psTO2_1.setString(2, c_f_name);
            psTO2_1.setString(3, c_l_name);
            psTO2_1.setString(4, c_tax_id);
            psTO2_1.execute(); hStats.incOperation();

            psTO3_1b.setString(1, symbol);
            rs = psTO3_1b.executeQuery(); hStats.incOperation();
            if (rs.next()){
                s_co_id = rs.getLong("s_co_id");
            }
            psTO3_2b.setLong(1, s_co_id);

            psTO3_2b.execute(); hStats.incOperation();
            psTO3_3.setString(1, symbol);
            rs=psTO3_3.executeQuery(); hStats.incOperation();
            double trade_price = 0;
            if(rs.next()){
                trade_price = rs.getDouble("lt_price");
            }
            else {return toResult;}
            //get hold_qty
            psTO3_5.setLong(1, ca_c_id);
            psTO3_5.setString(2, symbol);
            rs = psTO3_5.executeQuery(); hStats.incOperation();
            if (rs.next()){
                hold_qty = rs.getInt("hs_qty");
            }

            //--------------------------------------//
            psTO3_6a.setLong(1, ca_c_id);
            psTO3_6a.setString(2, symbol);
            psTO3_6b.setLong(1, ca_c_id);
            psTO3_6b.setString(2, symbol);
            List holdList =new ArrayList<Map<String, Object>>();

            if (type_is_sell) {
                if (hold_qty > 0 ){
                    if (is_lifo){
                        rs = psTO3_6a.executeQuery(); hStats.incOperation();
                        int i =0;
                        Map<String,Object> resrow = new HashMap<>();
                        while (rs.next()){
                            resrow.put("h_qty", rs.getInt("h_qty"));
                            holdList.add(i, resrow);
                            i++;
                        }
                    }
                    else {
                        rs = psTO3_6b.executeQuery(); hStats.incOperation();
                        int i =0;
                        Map<String,Object> resrow = new HashMap<>();
                        while (rs.next()){
                            resrow.put("h_qty", rs.getInt("h_qty"));
                            holdList.add(i, resrow);
                            i++;
                        }
                    }
                    if (!holdList.isEmpty()){
                        for(int i=0; i<holdList.size(); i++){
                            Map entry = (Map) holdList.get(i);
                            if (Integer.parseInt(entry.get("h_qty").toString()) > needed_qty){
                                buy_value += needed_qty * Double.parseDouble(entry.get("h_price").toString());
                                sell_value += needed_qty * trade_price;
                                needed_qty = 0;
                                continue;
                            }
                            else {
                                buy_value += needed_qty * Double.parseDouble(entry.get("h_price").toString());
                                sell_value += needed_qty * trade_price;
                                needed_qty = needed_qty - Integer.parseInt(entry.get("h_qty").toString());
                            }
                        }
                    }
                }
            }
            else {
                if (hold_qty <0) { // Existing short position to buy
                    if (is_lifo) {
                        rs = psTO3_6a.executeQuery(); hStats.incOperation();
                        int i =0;
                        Map<String,Object> resrow = new HashMap<>();
                        while (rs.next()){
                            resrow.put("h_qty", rs.getInt("h_qty"));
                            holdList.add(i, resrow);
                            i++;
                        }
                    }
                    else {
                        rs = psTO3_6b.executeQuery(); hStats.incOperation();
                        int i =0;
                        Map<String,Object> resrow = new HashMap<>();
                        while (rs.next()){
                            resrow.put("h_qty", rs.getInt("h_qty"));
                            holdList.add(i, resrow);
                            i++;
                        }
                    }
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

            String status_id = "";
            if (type_is_market) {status_id = "SBMT"; }
            else {status_id = "PNDG";}
            String charge_amount = "10.60";
            String comm_amount = "0.70";
            String tradePriceStr = Double.toString(trade_price).replace(",", ".");

            //String trade_id = dbObject.EXEC_QUERY("SELECT NEXTVAL('SEQ_TRADE_ID')");
            String trade_id="";
            rs = st.executeQuery("SELECT NEXTVAL('SEQ_TRADE_ID')");
            if(rs.next()){
                trade_id = rs.getString(1);
            }

            String  sqlTOF4_1 = String.format(
                    "INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, " +
                            "                  t_s_symb, t_qty, t_bid_price, t_ca_id, " +
                            "                  t_exec_name, t_trade_price, t_chrg, t_comm, " +
                            "                  t_tax, t_lifo) " +
                            "VALUES (%s, now(), '%s', '%s', %s, '%s', " +
                            "        %d, %s, %s, '%s', NULL, %s, %s, 0, %s) ",
                    trade_id, status_id, t_tt_id, t_is_cash, symbol, trade_qty, tradePriceStr, acct_id,
                    exec_name, charge_amount, comm_amount, is_lifo);
            st.executeUpdate(sqlTOF4_1);
            hStats.incWriteOp();

            String  sqlTOF4_2 = String.format(
                    "INSERT INTO trade_request(tr_t_id, tr_tt_id, tr_s_symb, tr_qty, " +
                            "                          tr_bid_price, tr_b_id) " +
                            "VALUES (%s, '%s', '%s', %d, %s, %s)",trade_id, t_tt_id, symbol, trade_qty, tradePriceStr,
                    broker_id);
            st.executeUpdate(sqlTOF4_2);
            hStats.incWriteOp();

            String  sqlTOF4_3 = String.format(
                    "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) " +
                            "VALUES(%s, now(), '%s')", trade_id, status_id);
            st.executeUpdate(sqlTOF4_3);
            hStats.incWriteOp();
            st.getConnection().commit();
            toResult[0] = trade_id;
            toResult[1] = tradePriceStr;

            st.getConnection().setAutoCommit(true);
            rs.close();         rs      =null;
            psTO1.close();      psTO1   =null;
            psTO1_2.close();    psTO1_2 =null;
            psTO1_3.close();    psTO1_3 =null;
            psTO2_1.close();    psTO2_1 =null;
            psTO3_1b.close();   psTO3_1b=null;
            psTO3_2b.close();   psTO3_2b=null;
            psTO3_3.close();    psTO3_3 =null;
            psTO3_5.close();    psTO3_5 =null;
            psTO3_6a.close();   psTO3_6a=null;
            psTO3_6b.close();   psTO3_6b=null;

        }catch(Exception e){
            e.printStackTrace();
            try{
                st.getConnection().rollback();
                st.getConnection().setAutoCommit(true);
                rs.close();
            }catch (SQLException exc){
                exc.printStackTrace();
            }
            toResult[0]="";
            toResult[1]="";
            return toResult;
        }
        hStats.increment(3);
        long endTime = System.currentTimeMillis();
        hStats.insertTime(3, endTime - startTime);
        return toResult;
    }

    public static void tradeResult(Statement st, String trade_id, double trade_price){
        ResultSet rs = null;
        long startTime = System.currentTimeMillis();
        try{
            st.getConnection().setAutoCommit(false);
            String trFrame1_1 = String.format(
                    "SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg " +
                            "FROM trade " +
                            "WHERE t_id = %s", trade_id);
            rs = st.executeQuery(trFrame1_1);
            hStats.incOperation();

            String acct_id 		="";
            String type_id 		="";
            String symbol 		="";
            String trade_qty 	="";
            String charge		="";

            if(rs.next()){
                acct_id     = rs.getString("t_ca_id");
                type_id     = rs.getString("t_tt_id");
                symbol      = rs.getString("t_s_symb");
                trade_qty   = rs.getString("t_qty");
                charge      = rs.getString("t_chrg");
            }

            String trFrame1_2 = String.format(
                    "SELECT tt_name " +
                            "FROM trade_type " +
                            "WHERE tt_id = '%s'", type_id);
            st.execute(trFrame1_2);
            hStats.incOperation();

            String trFrame1_3 = String.format(
                    "SELECT hs_qty " +
                            "FROM holding_summary " +
                            "WHERE hs_ca_id = %s " +
                            "  AND hs_s_symb = '%s'", acct_id, symbol);
            rs = st.executeQuery(trFrame1_3);
            hStats.incOperation();

            int hold_qty = 0;
            if (rs.next()){
                hold_qty = rs.getInt("hs_qty");
            }

            java.util.Date date= new java.util.Date();
            java.util.Date trade_date = new Timestamp(date.getTime());
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
                            "WHERE ca_id = %s " , acct_id);
            rs = st.executeQuery(trFrame2_1);
            hStats.incOperation();

            String broker_id    ="";
            String cust_id 		="";
            String tax_status 	="";

            if (rs.next()){
                broker_id   = rs.getString("ca_b_id");
                cust_id     = rs.getString("ca_c_id");
                tax_status  = rs.getString("ca_tax_st");
            }

            List holdList =new ArrayList<Map<String, Object>>();
            if (type_is_sell){
                if (hold_qty  == 0) {
                    String trFrame2_2a = String.format(
                            "INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
                                    "VALUES(%s, '%s', %d)", acct_id, symbol, (-1)*Integer.parseInt(trade_qty));
                    st.executeUpdate(trFrame2_2a);
                    hStats.incWriteOp();
                }
                else if (hold_qty!=Integer.parseInt(trade_qty)) {
                    String trFrame2_2b = String.format(
                            "UPDATE holding_summary " +
                                    "SET hs_qty = %d " +
                                    "WHERE hs_ca_id = %s  " +
                                    "  AND hs_s_symb = '%s'", hold_qty - Integer.parseInt(trade_qty),
                            acct_id, symbol);
                    st.executeUpdate(trFrame2_2b);
                    hStats.incWriteOp();
                }
                if (hold_qty > 0) {
                    String trFrame2_3a = String.format(
                            "SELECT h_t_id, h_qty, h_price " +
                                    "FROM holding " +
                                    "WHERE h_ca_id = %s " +
                                    "  AND h_s_symb = '%s' " +
                                    "ORDER BY h_dts DESC ", acct_id, symbol);
                    rs = st.executeQuery(trFrame2_3a);
                    hStats.incOperation();
                    int iList =0;
                    Map<String,Object> resrow = new HashMap<>();
                    while (rs.next()){
                        resrow.put("h_qty", rs.getInt("h_qty"));
                        holdList.add(iList, resrow);
                        resrow.put("h_t_id",rs.getLong("h_t_id"));
                        holdList.add(iList, resrow);
                        resrow.put("h_price", rs.getString("h_price"));
                        holdList.add(iList, resrow);
                        iList++;
                    }

                    if (!holdList.isEmpty()){
                        // Liquidate existing holdings. Note that more than
                        // 1 HOLDING record can be deleted here since customer
                        // may have the same security with differing prices.
                        for(int i=0; i<holdList.size(); i++){
                            Map entry = (Map) holdList.get(i);
                            if ( Integer.parseInt(entry.get("h_qty").toString()) > needed_qty){
                                //Selling some of the holdings


                                String trFrame2_4ai=String.format(
                                        "UPDATE holding_history SET hh_before_qty=%s, hh_after_qty=%s WHERE " +
                                                "hh_h_t_id=%s AND hh_t_id=%s ",
                                        entry.get("h_qty"),
                                        (hold_qty-needed_qty),
                                        entry.get("h_t_id"),
                                        trade_id);

                                String trFrame2_4aii=String.format(
                                        "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) " +
                                                " SELECT  %s, %s, %s, %d " +
                                                " WHERE NOT EXISTS (SELECT 1 FROM holding_history WHERE hh_h_t_id=%s)",
                                        entry.get("h_t_id"),
                                        trade_id,
                                        entry.get("h_qty"),
                                        (hold_qty - needed_qty),
                                        entry.get("h_t_id")
                                        );


                                String trFrame2_4aa = String.format(
                                        "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
                                                "                            hh_after_qty) " +
                                                "VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, entry.get("h_qty"),
                                        (hold_qty - needed_qty));
                                //dbObject.DML(trFrame2_4a);
                                st.executeUpdate(trFrame2_4ai);
                                st.executeUpdate(trFrame2_4aii);
                                hStats.incWriteOp();

                                String trFrame2_5a = String.format(
                                        "UPDATE holding " +
                                                "SET h_qty = %d " +
                                                "WHERE h_t_id = %s", hold_qty-needed_qty, entry.get("h_t_id"));
                                //dbObject.DML(trFrame2_5a);
                                st.executeUpdate(trFrame2_5a);
                                hStats.incWriteOp();
                                buy_value += needed_qty * Double.parseDouble(entry.get("h_price").toString());
                                sell_value += needed_qty * trade_price;
                                needed_qty = 0;
                                continue;
                            }
                            else {
                                //selling all holdings
                                String trFrame2_4ba = String.format(
                                        "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
                                                "                            hh_after_qty) " +
                                                "VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, entry.get("h_qty"), 0);
                                //dbObject.DML(trFrame2_4a);
                                String trFrame2_4bi=String.format(
                                        "UPDATE holding_history SET hh_before_qty=%s, hh_after_qty=%s WHERE " +
                                                "hh_h_t_id=%s AND hh_t_id=%s ",
                                        entry.get("h_qty"),
                                        0,
                                        entry.get("h_t_id"),
                                        trade_id);

                                String trFrame2_4bii=String.format(
                                        "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) " +
                                                " SELECT  %s, %s, %s, %d " +
                                                " WHERE NOT EXISTS (SELECT 1 FROM holding_history WHERE hh_h_t_id=%s)",
                                        entry.get("h_t_id"),
                                        trade_id,
                                        entry.get("h_qty"),
                                        0,
                                        entry.get("h_t_id")
                                );
                                st.executeUpdate(trFrame2_4bi);
                                st.executeUpdate(trFrame2_4bii);
                                hStats.incWriteOp();
                                String trFrame2_5b = String.format(
                                        "DELETE FROM holding " +
                                                "WHERE h_t_id = %s", entry.get("h_t_id"));
                                //dbObject.DML(trFrame2_5b);
                                st.executeUpdate(trFrame2_5b);
                                hStats.incWriteOp();

                                buy_value += hold_qty * Double.parseDouble( entry.get("h_price").toString());
                                sell_value += hold_qty * trade_price;
                                needed_qty = needed_qty -  Integer.parseInt(entry.get("h_qty").toString());
                            }
                        }//close
                    }
                }
                if (needed_qty > 0) {
                    String trFrame2_4a = String.format(
                            "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
                                    "                            hh_after_qty) " +
                                    "VALUES(%s, %s, %s, %d)", trade_id, trade_id, 0, (-1)*needed_qty);
                    //dbObject.DML(trFrame2_4a);
                    st.executeUpdate(trFrame2_4a);
                    hStats.incWriteOp();

                    String tradePriceStr = Double.toString(trade_price).replace(",", ".");
                    String trFrame2_7a = String.format(
                            "INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, " +
                                    "                    h_qty) " +
                                    "VALUES (%s, %s, '%s', '%s', %s, %d)",
                            trade_id, acct_id, symbol, trade_dts, tradePriceStr, (-1)*needed_qty);
                    //dbObject.DML(trFrame2_7a);
                    st.executeUpdate(trFrame2_7a);
                    hStats.incWriteOp();

                }
                else if (hold_qty == Integer.parseInt(trade_qty)) {
                    String trFrame2_7b = String.format(
                            "DELETE FROM holding_summary " +
                                    "WHERE hs_ca_id = %s " +
                                    "  AND hs_s_symb = '%s'", acct_id, symbol);
                    //dbObject.DML(trFrame2_7b);
                    st.executeUpdate(trFrame2_7b);
                    hStats.incWriteOp();
                }
            }//end if type_is_sell
            else{
                if (hold_qty == 0){
                    String trFrame2_8a = String.format(
                            "INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
                                    "VALUES (%s, '%s', %s)", acct_id, symbol, trade_qty);
                    st.executeUpdate(trFrame2_8a);
                    hStats.incWriteOp();
                }
                else{
                    if(-hold_qty != Integer.parseInt(trade_qty)){
                        String trFrame2_8b = String.format(
                                "UPDATE holding_summary " +
                                        "SET hs_qty = %s " +
                                        "WHERE hs_ca_id = %s " +
                                        "  AND hs_s_symb = '%s'", trade_qty, acct_id, symbol);
                        st.executeUpdate(trFrame2_8b);
                        hStats.incWriteOp();
                    }
                }

                if (hold_qty < 0){
                    String trFrame2_3a = String.format(
                            "SELECT h_t_id, h_qty, h_price " +
                                    "FROM holding " +
                                    "WHERE h_ca_id = %s " +
                                    "  AND h_s_symb = '%s' " +
                                    "ORDER BY h_dts DESC ", acct_id, symbol);
                    rs = st.executeQuery(trFrame2_3a);
                    hStats.incOperation();
                    int iList =0;
                    Map<String,Object> resrow = new HashMap<>();
                    while (rs.next()){
                        resrow.put("h_qty", rs.getInt("h_qty"));
                        holdList.add(iList, resrow);
                        resrow.put("h_t_id",rs.getLong("h_t_id"));
                        holdList.add(iList, resrow);
                        resrow.put("h_price", rs.getString("h_price"));
                        holdList.add(iList, resrow);
                        iList++;
                    }
                    for(int i=0; i<holdList.size(); i++){
                        Map entry = (Map) holdList.get(i);
                        if (Integer.parseInt(entry.get("h_qty").toString()) + needed_qty < 0) {
                            //Bying back some of the short sell
                            String trFrame2_4aa = String.format(
                                    "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, " +
                                            "                            hh_after_qty) " +
                                            "VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, 0, Integer.parseInt(entry.get("h_qty").toString()) + needed_qty);

                            String trFrame2_4ai=String.format(
                                    "UPDATE holding_history SET hh_before_qty=%s, hh_after_qty=%s WHERE " +
                                            "hh_h_t_id=%s AND hh_t_id=%s ",
                                    0,
                                    Integer.parseInt(entry.get("h_qty").toString()) + needed_qty,
                                    entry.get("h_t_id"),
                                    trade_id);

                            String trFrame2_4aii=String.format(
                                    "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) " +
                                            " SELECT  %s, %s, %s, %d " +
                                            " WHERE NOT EXISTS (SELECT 1 FROM holding_history WHERE hh_h_t_id=%s)",
                                    entry.get("h_t_id"),
                                    trade_id,
                                    0,
                                    Integer.parseInt(entry.get("h_qty").toString()) + needed_qty,
                                    entry.get("h_t_id")
                            );

                            st.executeUpdate(trFrame2_4ai);
                            st.executeUpdate(trFrame2_4ai);
                            hStats.incWriteOp();
                            String trFrame2_5a = String.format(
                                    "UPDATE holding " +
                                            "SET h_qty = %d " +
                                            "WHERE h_t_id = %s", hold_qty+needed_qty, entry.get("h_t_id"));
                            st.executeUpdate(trFrame2_5a);
                            hStats.incWriteOp();
                            sell_value 	+= needed_qty*Double.parseDouble(entry.get("h_price").toString());
                            buy_value 	+= needed_qty*trade_price;
                            needed_qty 	= 0;
                            continue;
                        }
                        else { //buying back all of the short sell
                            String trFrame2_4aa = String.format(
                                    "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) " +
                                            "VALUES(%s, %s, %s, %d)", entry.get("h_t_id"), trade_id, entry.get("h_qty"), 0);

                            String trFrame2_4bi=String.format(
                                    "UPDATE holding_history SET hh_before_qty=%s, hh_after_qty=%s WHERE " +
                                            "hh_h_t_id=%s AND hh_t_id=%s ",
                                    entry.get("h_qty"),
                                    0,
                                    entry.get("h_t_id"),
                                    trade_id);

                            String trFrame2_4bii=String.format(
                                    "INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) " +
                                            " SELECT  %s, %s, %s, %d " +
                                            " WHERE NOT EXISTS (SELECT 1 FROM holding_history WHERE hh_h_t_id=%s)",
                                    entry.get("h_t_id"),
                                    trade_id,
                                    entry.get("h_qty"),
                                    0,
                                    entry.get("h_t_id")
                            );

                            st.executeUpdate(trFrame2_4bi);
                            st.executeUpdate(trFrame2_4bii);
                            hStats.incWriteOp();
                            String trFrame2_5b = String.format(
                                    "DELETE FROM holding " +
                                            "WHERE h_t_id = %s", entry.get("h_t_id"));
                            st.executeUpdate(trFrame2_5b);
                            hStats.incWriteOp();
                            // Make hold_qty positive for easy calculations
                            hold_qty = 0-hold_qty;
                            sell_value += hold_qty * Double.parseDouble(entry.get("h_price").toString());
                            buy_value += hold_qty * trade_price;
                            needed_qty = needed_qty - hold_qty;
                        }
                    }
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
                    st.executeUpdate(trFrame2_4a);
                    hStats.incWriteOp();

                    String trFrame2_7a = String.format(
                            "INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, " +
                                    "                    h_qty) " +
                                    "VALUES (%s, %s, '%s', '%s', %f, %d)",
                            trade_id, acct_id, symbol, trade_dts, trade_price, needed_qty);
                    st.executeUpdate(trFrame2_7a);
                    hStats.incWriteOp();
                }
                else if (-hold_qty == Integer.parseInt(trade_qty)){
                    String trFrame2_5b = String.format(
                            "DELETE FROM holding_summary " +
                                    "WHERE h_ca_id = %s and hs_s_symb = '%s'", acct_id, symbol);
                    st.executeUpdate(trFrame2_5b);
                    hStats.incWriteOp();
                }
            }//end buy
            ///////////////////////////
            //FRAME 3
            //init tax_ammount to 0.0
            double tax_amount = 0.0;
            String trFrame3_1 = String.format(
                    "SELECT SUM(tx_rate) " +
                            "FROM taxrate " +
                            "WHERE tx_id IN (SELECT cx_tx_id " +
                            "                FROM customer_taxrate " +
                            "                WHERE cx_c_id = %s) ", cust_id);
            rs = st.executeQuery(trFrame3_1);
            hStats.incOperation();
            double tax_rates = 0;
            if (rs.next()){
                tax_rates = rs.getDouble("sum");
            }
            tax_amount = (sell_value - buy_value) * tax_rates;

            String trFrame3_2 = String.format(
                    "UPDATE trade " +
                            "SET t_tax = %f " +
                            "WHERE t_id = %s", tax_amount, trade_id);
            st.executeUpdate(trFrame3_2);
            hStats.incWriteOp();

            //Finished frame 3.
            //Go for frame 4..

            String trFrame4_1 = String.format(
                    "SELECT s_ex_id, s_name " +
                            "FROM security " +
                            "WHERE s_symb = '%s'", symbol);
            //Map sexid = dbObject.QUERY2MAP(trFrame4_1);
            String s_ex_id = "";//sexid.get("s_ex_id").toString();
            String s_name = "";//sexid.get("s_name").toString();
            rs = st.executeQuery(trFrame4_1);
            hStats.incOperation();
            if (rs.next()){
                s_ex_id = rs.getString("s_ex_id");
                s_name = rs.getString("s_name");
            }

            String trFrame4_2 = String.format(
                    "SELECT c_tier " +
                            "FROM customer " +
                            "WHERE c_id = %s", cust_id);
            rs = st.executeQuery(trFrame4_2);
            hStats.incOperation();
            String c_tier = "";
            if (rs.next()){
                c_tier = rs.getString("c_tier");
            }


            String trFrame4_3 = String.format(
                    "SELECT cr_rate " +
                            "FROM commission_rate " +
                            "WHERE cr_c_tier = %s " +
                            "  AND cr_tt_id = '%s' " +
                            "  AND cr_ex_id = '%s' " +
                            "  AND cr_to_qty-cr_from_qty >= %s " +
                            "  AND cr_to_qty >= %s " +
                            "LIMIT 1", c_tier, type_id, s_ex_id, trade_qty, trade_qty);
            //String comm_rate = dbObject.QUERY2MAP(trFrame4_3).get("cr_rate").toString();
            rs = st.executeQuery(trFrame4_3);
            hStats.incOperation();
            String comm_rate = "";
            if (rs.next()){
                comm_rate = rs.getString("cr_rate");
            }
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
            st.executeUpdate(trFrame5_1);
            hStats.incWriteOp();

            String trFrame5_2 = String.format(
                    "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) " +
                            "VALUES (%s, '%s', '%s')", trade_id, trade_dts, st_completed_id);
            st.executeUpdate(trFrame5_2);
            hStats.incWriteOp();

            String trFrame5_3 = String.format(
                    "UPDATE broker " +
                            "SET b_comm_total = b_comm_total + %f, " +
                            "    b_num_trades = b_num_trades + 1 " +
                            "WHERE b_id = %s", comm_amount, broker_id);
            st.executeUpdate(trFrame5_3);
            hStats.incWriteOp();

            //END OF FRAME 5
            //GO FOR FRAME 6
            boolean cash_type = hihUtil.testRndGen.nextBoolean();
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
            st.executeUpdate(trFrame6_1);
            hStats.incWriteOp();

            String trFrame6_2 = String.format(
                    "UPDATE customer_account " +
                            "SET ca_bal = ca_bal + (%f) " +
                            "WHERE ca_id = %s", se_amount, acct_id);
            st.executeUpdate(trFrame6_2);
            hStats.incWriteOp();

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
            s_name = s_name.replace("'","");
            String trFrame6_3 = String.format(
                    "INSERT INTO cash_transaction(ct_dts, ct_t_id, ct_amt, ct_name) " +
                            "VALUES ('%s', %s, %f, e'%s %s shared of %s')", trade_dts, trade_id, se_amount, type_name,
                    trade_qty, s_name);
            //System.out.println(trFrame6_3);
            st.executeUpdate(trFrame6_3);
            hStats.incWriteOp();

            String trFrame6_4 = String.format(
                    "SELECT ca_bal " +
                            "FROM customer_account " +
                            "WHERE ca_id = %s", acct_id);
            st.executeQuery(trFrame6_4);
            hStats.incWriteOp();




            ///////////////////////////
            st.getConnection().commit();
            st.getConnection().setAutoCommit(true);
            rs.close(); rs = null;
        }catch (Exception e){
            e.printStackTrace();
            try{
                st.getConnection().rollback();
                st.getConnection().setAutoCommit(true);
                rs.close(); rs = null;
            }catch (SQLException exc){
                exc.printStackTrace();
            }
            return;
        }
        long endTime = System.currentTimeMillis();
        hStats.insertTime(4, endTime-startTime);
        hStats.increment(4);
    }

    public static void tradeCleanup(Statement st){
        //:TODO
        //RUN TRADE CLEANUP FRAME AS SHOWN BELOW TO CLEAN TRADE_REQUEST TABLE
        //USING: select min(tr_t_id) from trade_request;
        //select * from TradeCleanupFrame1('CNCL', 'PNDG', 'SBMT', 200000000070836);
        ResultSet rs =null;
        PreparedStatement ps = null;
        try{
            rs = st.executeQuery("select min(tr_t_id) from trade_request");
            Long tr_t_id = 0L;
            if (rs.next()){
                tr_t_id = rs.getLong("min");
            }
            String clean =  "select * from TradeCleanupFrame1('CNCL', 'PNDG', 'SBMT', ?)";
            ps = st.getConnection().prepareStatement(clean);
            ps.setLong(1, tr_t_id);
            ps.execute();
            ////////////
            ps.close();
            rs.close();
        }catch(Exception e){
            e.printStackTrace();
            try{
                ps.close();
                rs.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("Database Cleaned");
    }

}
