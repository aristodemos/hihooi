package hih;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by mariosp on 31/1/16.
 */
public class hihTransactions{

    static BenStatistics hStats = new BenStatistics();

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
                "AND b_name = ANY ('{%s}')" +			//'%s' " +//"AND b_name =
                "AND sc_name = '%s' " +
                "GROUP BY b_name " +
                "ORDER BY 2 DESC", activeBrokersStr, sector_name);  //actoive.brokers.get(i)
        long startTime = System.currentTimeMillis();
        util.QUERY(query);
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
                        "WHERE T_CA_ID = %1$s " +
                        "ORDER BY T_DTS desc LIMIT 10) as T, " +
                        "TRADE, TRADE_HISTORY, STATUS_TYPE " +
                        "WHERE T_ID = ID " +
                        "AND TH_T_ID = T_ID " +
                        "AND ST_ID = TH_ST_ID " +
                        "ORDER BY TH_DTS desc " +
                        "LIMIT 30";

        long startTime = System.currentTimeMillis();
        try{
            util.QUERY(query1);
            hStats.incOperation();
            String c_ad_id = util.QUERY2MAP(query2a).get("c_ad_id").toString();
            hStats.incOperation();
            util.QUERY(String.format(query2b, c_ad_id));
            hStats.incOperation();
        }catch(Exception e){e.printStackTrace();return;}
        long endTime = System.currentTimeMillis();
        hStats.insertTime(1, endTime - startTime);
        hStats.increment(1);
    }

    public void marketFeedFrame(hihUtil util) {

        String qSymbSet = "select distinct tr_s_symb, random() from TRADE_REQUEST order by random() limit 20";
        String tradeQtQuery = "select tr_qty from trade_request where tr_s_symb = %1$s limit 1";
        //String qMF1pre = "SELECT * from  LAST_TRADE WHERE LT_S_SYMB = %1$s FOR UPDATE";
        String qMF1 = "UPDATE LAST_TRADE " +
                "SET LT_PRICE = %1$s, " +
                "LT_VOL = LT_VOL + %2$s, " +
                "LT_DTS = now() " +
                "WHERE LT_S_SYMB = '%3$s' ";
        //, priceQuote.get(i), tradeQuantity.get(i), activeSymbolsSet.get(i));

        String qMF2 = "SELECT TR_T_ID "+
                "FROM TRADE_REQUEST " +
                "WHERE TR_S_SYMB = %1$s and " +
                "((TR_TT_ID = 'TSL' and TR_BID_PRICE >= %2$s) or " +
                "(TR_TT_ID = 'TLS' and TR_BID_PRICE <= %2$s) or " +
                "(TR_TT_ID = 'TLB' and TR_BID_PRICE >= %2$s))";
        //,activeSymbolsSet.get(i),priceQuote.get(i),priceQuote.get(i),priceQuote.get(i));

        String qMF3 =   "UPDATE TRADE " + "SET T_DTS = now(), T_ST_ID = 'SBMT' WHERE T_ID = %1$s";
        //, request_list.get(j).get("tr_t_id"));
        String qMF4 =   "DELETE FROM TRADE_REQUEST WHERE TR_T_ID = %1$s";
        //, request_list.get(j).get("tr_t_id"));
        String qMF5 =   "INSERT INTO TRADE_HISTORY VALUES (%1$s, now(), 'SBMT')";
        //, request_list.get(j).get("tr_t_id"));

        int numberOfSymbols = 20;
        List<String> activeSymbolSet = new ArrayList();

        long startTime = System.currentTimeMillis();
        try{
            activeSymbolSet = util.QUERY2LST(qSymbSet);
            hStats.incOperation();
            if (activeSymbolSet.size()<1){
                return;
            }

            if (activeSymbolSet.size() < numberOfSymbols) {numberOfSymbols = activeSymbolSet.size();}
            activeSymbolSet = hihUtil.randomSample(activeSymbolSet, numberOfSymbols);

            ArrayList<Double> priceQuote = new ArrayList<Double>(numberOfSymbols);
            for (int i=0; i<numberOfSymbols; i++){
                double low 	= hihSerializedData.pricesDM.get(activeSymbolSet.get(i)).get(0);
                double high = hihSerializedData.pricesDM.get(activeSymbolSet.get(i)).get(1);
                double randInt = Math.abs(util.testRndGen.nextDouble());
                priceQuote.add(i, 0.7*low + randInt*(1.3*high-low) );
            }

            ArrayList<String> tradeQuantity= new ArrayList<String>(numberOfSymbols);
            for (int i=0; i<numberOfSymbols; i++){
                hStats.incOperation();
                tradeQuantity.add(i, util.EXEC_QUERY(String.format(tradeQtQuery, activeSymbolSet.get(i))));
            }

            for (int i=0; i<numberOfSymbols; i++) {
                util.START_TX();

                util.DML(String.format(qMF1, priceQuote.get(i), tradeQuantity.get(i), activeSymbolSet.get(i)));
                hStats.incWriteOp();


                List<Map<String, Object>> request_list = null;
                request_list = util.QUERY2LST(String.format(qMF2, activeSymbolSet.get(i), priceQuote.get(i)));
                hStats.incOperation();

                for (int j=0; j<request_list.size();j++) {

                    util.DML(String.format(qMF3,request_list.get(j).get("tr_t_id")));
                    hStats.incWriteOp();

                    util.DML(String.format(qMF4, request_list.get(j).get("tr_t_id")));
                    hStats.incWriteOp();

                    util.DML(String.format(qMF5, request_list.get(j).get("tr_t_id")));
                    hStats.incWriteOp();
                }
                util.TCL("commit");
            }
        }
        catch(Exception e){
            e.printStackTrace();
            util.TCL("rollback");
            return;
        }
        long endTime = System.currentTimeMillis();
        hStats.insertTime(2, endTime - startTime);
        hStats.increment(2);
    }

    public void tradeStatus(hihUtil util) {

        String acct_id =  hihSerializedData.all_acct_ids.get(util.testRndGen.nextInt(hihSerializedData.all_acct_ids.size()))
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
            util.EXEC_QUERY(sqlTSF1_1);
            hStats.incOperation();

            util.EXEC_QUERY(sqlTSF1_2);
            hStats.incOperation();
        }
        catch(Exception e){e.printStackTrace();return;}
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

            Map values = util.QUERY2MAP(sdf1_1);
            hStats.incOperation();
            String co_id = values.get("co_id").toString();

            util.QUERY(String.format(sdf1_2, co_id,valRand));
            hStats.incOperation();

            util.QUERY(String.format(sdf1_3, co_id, valRand));
            hStats.incOperation();

            util.QUERY(sdf1_4);
            hStats.incOperation();

            util.QUERY(sdf1_5);
            hStats.incOperation();

            util.QUERY(String.format(sdf1_7, co_id, valRand));
            hStats.incOperation();

        }
        catch(Exception e){e.printStackTrace();return;}
        long endTimer = System.currentTimeMillis();
        hStats.insertTime(6, endTimer-startTime);
        hStats.increment(6);
    }

    public String[] tradeOrder(hihUtil util) {

        String toResult[] = new String[2];

        String acct_id =  hihSerializedData.all_acct_ids.get(util.testRndGen.nextInt(hihSerializedData.all_acct_ids.size()))
                .toString();
        String symbol = hihSerializedData.all_symbols.get(util.testRndGen.nextInt(hihSerializedData.all_symbols.size()));

        int trade_qty = util.testRndGen.nextInt(800)+100;

        boolean type_is_market 	= util.testRndGen.nextBoolean();
        boolean type_is_sell	= util.testRndGen.nextBoolean();

        String t_tt_id = "";
        if (type_is_market){
            if (type_is_sell) {t_tt_id = "TMS";}
            else {t_tt_id = "TMB";}
        }
        else {
            if (type_is_sell) {t_tt_id = "TLS";}
            else {t_tt_id = "TLB";}
        }
        boolean is_lifo		= util.testRndGen.nextBoolean();
        boolean t_is_cash	= util.testRndGen.nextBoolean();

        // Get account, customer, and broker information into a Map
        String  sqlTOF1_1 = String.format("SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st " +
                "FROM customer_account " +
                "WHERE ca_id = %s", acct_id); //acct_id

        String  sqlTOF1_2 = "SELECT c_f_name, c_l_name, c_tier, c_tax_id " +
                "FROM customer " +
                "WHERE c_id = %1$s"; //, output1.get("ca_c_id"));

        String  sqlTOF1_3 = "SELECT b_name " +
                "FROM Broker  " +
                "WHERE b_id = %1$s";
        //, broker_id);

        String  sqlTOF2_1 = "SELECT ap_acl  " +
                "FROM account_permission " +
                "WHERE ap_ca_id = %1$s " +
                "  AND ap_f_name = '%2$s' " +
                "  AND ap_l_name = '%3$s' " +
                "  AND ap_tax_id = '%4$s' ";
        //, acct_id, output2.get("c_f_name"), output2.get("c_l_name"), output2   .get("c_tax_id"));

        String  sqlTOF3_1b = String.format("SELECT s_co_id, s_ex_id, s_name " +
                "FROM security " +
                "WHERE s_symb = '%s' ", symbol);

        String  sqlTOF3_2b = "SELECT co_name " +
                "FROM company " +
                "WHERE co_id = '%1$s'";
        //, output5.get("s_co_id"));

        String  sqlTOF3_3 = String.format("SELECT lt_price " +
                "FROM last_trade " +
                "WHERE lt_s_symb = '%s'", symbol);

        String  sqlTOF3_5 = String.format("SELECT hs_qty " +
                "FROM holding_summary " +
                "WHERE hs_ca_id = %s " +
                "  AND hs_s_symb = '%s'", acct_id, symbol);

        String  sqlTOF3_6a = String.format("SELECT h_qty, h_price " +
                "FROM holding " +
                "WHERE h_ca_id = %s " +
                "  AND h_s_symb = '%s' " +
                "ORDER BY h_dts DESC", acct_id, symbol);

        String  sqlTOF3_6b = String.format("SELECT h_qty, h_price " +
                "FROM holding " +
                "WHERE h_ca_id = %s " +
                "  AND h_s_symb = '%s' " +
                "ORDER BY h_dts ASC", acct_id, symbol);

        long startTime = System.currentTimeMillis();
        try{
            Map to1_1 = util.QUERY2MAP(sqlTOF1_1);
            hStats.incOperation();
            String exec_name    = to1_1.get("ca_name").toString();
            String ca_c_id      = to1_1.get("ca_c_id").toString();;
            String broker_id    = to1_1.get("ca_b_id").toString();;

            //
            double hold_price;
            double buy_value = 0.0, sell_value = 0.0;
            int hold_qty = 0, needed_qty = trade_qty;

            Map to1_2 = util.QUERY2MAP(String.format(sqlTOF1_2, ca_c_id));
            hStats.incOperation();

            String c_f_name     = to1_2.get("c_f_name").toString();
            String c_l_name     = to1_2.get("c_l_name").toString();
            String c_tax_id     = to1_2.get("c_tax_id").toString();

            util.EXEC_QUERY(String.format(sqlTOF1_3, broker_id));
            hStats.incOperation();

            util.EXEC_QUERY(String.format(sqlTOF2_1, acct_id, c_f_name, c_l_name, c_tax_id));
            hStats.incOperation();

            Map to3_1 = util.QUERY2MAP(sqlTOF3_1b);
            hStats.incOperation();
            String s_co_id        = to3_1.get("c_tax_id").toString();

            util.EXEC_QUERY(String.format(sqlTOF3_2b, s_co_id));
            hStats.incOperation();

            Map to3_3 = util.QUERY2MAP(sqlTOF3_3);
            hStats.incOperation();

            double trade_price =  Double.parseDouble(to3_3.get("lt_price").toString());

            //get hold_qty
            Map to3_5 = util.QUERY2MAP(sqlTOF3_5);
            hStats.incOperation();
            hold_qty = Integer.parseInt(to3_5.get("hs_qty").toString());
            //--------------------------------------//
            List holdList =new ArrayList<Map<String, Object>>();

            if (type_is_sell) {
                if (hold_qty > 0 ){
                    if (is_lifo){
                        holdList = util.QUERY2LST(sqlTOF3_6a);
                        hStats.incOperation();
                    }
                    else {
                        holdList = util.QUERY2LST(sqlTOF3_6b);
                        hStats.incOperation();
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
                        holdList = util.QUERY2LST(sqlTOF3_6a);
                        hStats.incOperation();
                    }
                    else {
                        holdList = util.QUERY2LST(sqlTOF3_6b);
                        hStats.incOperation();
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

            util.START_TX();
            String trade_id = util.EXEC_QUERY("SELECT NEXTVAL('SEQ_TRADE_ID')");


            String  sqlTOF4_1 = String.format(
                    "INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, " +
                            "                  t_s_symb, t_qty, t_bid_price, t_ca_id, " +
                            "                  t_exec_name, t_trade_price, t_chrg, t_comm, " +
                            "                  t_tax, t_lifo) " +
                            "VALUES (%s, now(), '%s', '%s', %s, '%s', " +
                            "        %d, %s, %s, '%s', NULL, %s, %s, 0, %s) ",
                    trade_id, status_id, t_tt_id, t_is_cash, symbol, trade_qty, tradePriceStr, acct_id,
                    exec_name, charge_amount, comm_amount, is_lifo);
            util.DML(sqlTOF4_1);
            hStats.incWriteOp();

            String  sqlTOF4_2 = String.format(
                    "INSERT INTO trade_request(tr_t_id, tr_tt_id, tr_s_symb, tr_qty, " +
                            "                          tr_bid_price, tr_b_id) " +
                            "VALUES (%s, '%s', '%s', %d, %s, %s)",trade_id, t_tt_id, symbol, trade_qty, tradePriceStr,
                    broker_id);
            util.DML(sqlTOF4_2);
            hStats.incWriteOp();

            String  sqlTOF4_3 = String.format(
                    "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) " +
                            "VALUES(%s, now(), '%s')", trade_id, status_id);
            util.DML(sqlTOF4_3);
            hStats.incWriteOp();

            util.TCL("commit");
            toResult[0] = trade_id;
            toResult[1] = tradePriceStr;

        }catch(Exception e){
            e.printStackTrace();
            util.TCL("rollback");
            toResult[0]="";
            toResult[1]="";
            return toResult;
        }
        hStats.increment(3);
        long endTime = System.currentTimeMillis();
        hStats.insertTime(3, endTime-startTime);
        return toResult;
    }

    public void tradeResult(hihUtil util, String trade_id, double trade_price){

        long startTime = System.currentTimeMillis();
        try{

            String trFrame1_1 = String.format(
                    "SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg " +
                            "FROM trade " +
                            "WHERE t_id = %s", trade_id);
            Map tr1_1 = util.QUERY2MAP(trFrame1_1);
            hStats.incOperation();

            String acct_id 		= tr1_1.get("t_ca_id").toString();
            String type_id 		= tr1_1.get("t_tt_id").toString();
            String symbol 		= tr1_1.get("t_s_symb").toString();
            String trade_qty 	= tr1_1.get("t_qty").toString();
            String charge		= tr1_1.get("t_chrg").toString();

            String trFrame1_2 = String.format(
                    "SELECT tt_name " +
                            "FROM trade_type " +
                            "WHERE tt_id = '%s'", type_id);
            util.EXEC_QUERY(trFrame1_2);
            hStats.incOperation();

            String trFrame1_3 = String.format(
                    "SELECT hs_qty " +
                            "FROM holding_summary " +
                            "WHERE hs_ca_id = %s " +
                            "  AND hs_s_symb = '%s'", acct_id, symbol);
            Map tr1_3 = util.QUERY2MAP(trFrame1_3);
            hStats.incOperation();

            int hold_qty = Integer.parseInt(tr1_3.get("hs_qty").toString());

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
                            "WHERE ca_id = %s " +
                            "FOR UPDATE", acct_id);
            Map initFrame3 = util.QUERY2MAP(trFrame2_1);

            List holdList =new ArrayList<Map<String, Object>>();
            if (type_is_sell){
                if (hold_qty  == 0) {
                    String trFrame2_2a = String.format(
                            "INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
                                    "VALUES(%s, '%s', %d)", acct_id, symbol, (-1)*Integer.parseInt(trade_qty));
                    util.DML(trFrame2_2a);
                }
                else if (hold_qty!=Integer.parseInt(trade_qty)) {
                    String trFrame2_2b = String.format(
                            "UPDATE holding_summary " +
                                    "SET hs_qty = %d " +
                                    "WHERE hs_ca_id = %s  " +
                                    "  AND hs_s_symb = '%s'", hold_qty - Integer.parseInt(trade_qty),
                            acct_id, symbol);
                    util.DML(trFrame2_2b);
                }
                if (hold_qty > 0) {
                    String trFrame2_3a = String.format(
                            "SELECT h_t_id, h_qty, h_price " +
                                    "FROM holding " +
                                    "WHERE h_ca_id = %s " +
                                    "  AND h_s_symb = '%s' " +
                                    "ORDER BY h_dts DESC ", acct_id, symbol);
                    holdList = util.QUERY2LST(trFrame2_3a);

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
                                util.DML(trFrame2_4ai);
                                util.DML(trFrame2_4aii);

                                String trFrame2_5a = String.format(
                                        "UPDATE holding " +
                                                "SET h_qty = %d " +
                                                "WHERE h_t_id = %s", hold_qty-needed_qty, entry.get("h_t_id"));
                                util.DML(trFrame2_5a);

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

                                util.DML(trFrame2_4bi);
                                util.DML(trFrame2_4bii);
                                String trFrame2_5b = String.format(
                                        "DELETE FROM holding " +
                                                "WHERE h_t_id = %s", entry.get("h_t_id"));
                                util.DML(trFrame2_5b);
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
                    util.DML(trFrame2_4a);

                    String tradePriceStr = Double.toString(trade_price).replace(",", ".");
                    String trFrame2_7a = String.format(
                            "INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, " +
                                    "                    h_qty) " +
                                    "VALUES (%s, %s, '%s', '%s', %s, %d)",
                            trade_id, acct_id, symbol, trade_dts, tradePriceStr, (-1)*needed_qty);
                    util.DML(trFrame2_7a);
                }
                else if (hold_qty == Integer.parseInt(trade_qty)) {
                    String trFrame2_7b = String.format(
                            "DELETE FROM holding_summary " +
                                    "WHERE hs_ca_id = %s " +
                                    "  AND hs_s_symb = '%s'", acct_id, symbol);
                    util.DML(trFrame2_7b);
                }
            }//end if type_is_sell
            else{
                if (hold_qty == 0){
                    String trFrame2_8a = String.format(
                            "INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty) " +
                                    "VALUES (%s, '%s', %s)", acct_id, symbol, trade_qty);
                    util.DML(trFrame2_8a);
                }
                else{
                    if(-hold_qty != Integer.parseInt(trade_qty)){
                        String trFrame2_8b = String.format(
                                "UPDATE holding_summary " +
                                        "SET hs_qty = %s " +
                                        "WHERE hs_ca_id = %s " +
                                        "  AND hs_s_symb = '%s'", trade_qty, acct_id, symbol);
                        util.DML(trFrame2_8b);
                    }
                }
                if (hold_qty < 0){
                    String trFrame2_3a = String.format(
                            "SELECT h_t_id, h_qty, h_price " +
                                    "FROM holding " +
                                    "WHERE h_ca_id = %s " +
                                    "  AND h_s_symb = '%s' " +
                                    "ORDER BY h_dts DESC ", acct_id, symbol);
                    holdList = util.QUERY2LST(trFrame2_3a);

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

                            util.DML(trFrame2_4ai);
                            util.DML(trFrame2_4aii);
                            String trFrame2_5a = String.format(
                                    "UPDATE holding " +
                                            "SET h_qty = %d " +
                                            "WHERE h_t_id = %s", hold_qty+needed_qty, entry.get("h_t_id"));
                            util.DML(trFrame2_5a);
                            sell_value 	+= needed_qty*Integer.parseInt(entry.get("h_price").toString());
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
                            util.DML(trFrame2_4bi);
                            util.DML(trFrame2_4bii);
                            String trFrame2_5b = String.format(
                                    "DELETE FROM holding " +
                                            "WHERE h_t_id = %s", entry.get("h_t_id"));
                            util.DML(trFrame2_5b);
                            // Make hold_qty positive for easy calculations
                            hold_qty = 0-hold_qty;
                            sell_value += hold_qty * Integer.parseInt(entry.get("h_price").toString());
                            buy_value += hold_qty * trade_price;
                            needed_qty = needed_qty - hold_qty;
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
                    util.DML(trFrame2_4a);

                    String trFrame2_7a = String.format(
                            "INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, " +
                                    "                    h_qty) " +
                                    "VALUES (%s, %s, '%s', '%s', %f, %d)",
                            trade_id, acct_id, symbol, trade_dts, trade_price, needed_qty);
                    util.DML(trFrame2_7a);
                }
                else if (-hold_qty == Integer.parseInt(trade_qty)){
                    String trFrame2_5b = String.format(
                            "DELETE FROM holding_summary " +
                                    "WHERE h_ca_id = %s and hs_s_symb = '%s'", acct_id, symbol);
                    util.DML(trFrame2_5b);
                }
            }//end buy
            ///////////
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
            double tax_rates = Double.parseDouble(util.QUERY2MAP(trFrame3_1).get("sum").toString());

            tax_amount = Math.abs(sell_value - buy_value) * tax_rates;

            String trFrame3_2 = String.format(
                    "UPDATE trade " +
                            "SET t_tax = %f " +
                            "WHERE t_id = %s", tax_amount, trade_id);
            util.DML(trFrame3_2);

            //Finished frame 3.
            //Go for frame 4..


            String trFrame4_1 = String.format(
                    "SELECT s_ex_id, s_name " +
                            "FROM security " +
                            "WHERE s_symb = '%s'", symbol);
            Map sexid = util.QUERY2MAP(trFrame4_1);
            String s_ex_id = sexid.get("s_ex_id").toString();
            String s_name = sexid.get("s_name").toString();

            String trFrame4_2 = String.format(
                    "SELECT c_tier " +
                            "FROM customer " +
                            "WHERE c_id = %s", cust_id);
            Map ctier = util.QUERY2MAP(trFrame4_2);
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
            String comm_rate = util.QUERY2MAP(trFrame4_3).get("cr_rate").toString();
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
            util.DML(trFrame5_1);

            String trFrame5_2 = String.format(
                    "INSERT INTO trade_history(th_t_id, th_dts, th_st_id) " +
                            "VALUES (%s, '%s', '%s')", trade_id, trade_dts, st_completed_id);
            util.DML(trFrame5_2);

            String trFrame5_3 = String.format(
                    "UPDATE broker " +
                            "SET b_comm_total = b_comm_total + %f, " +
                            "    b_num_trades = b_num_trades + 1 " +
                            "WHERE b_id = %s", comm_amount, broker_id);
            util.DML(trFrame5_3);

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
            util.DML(trFrame6_1);

            String trFrame6_2 = String.format(
                    "UPDATE customer_account " +
                            "SET ca_bal = ca_bal + (%f) " +
                            "WHERE ca_id = %s", se_amount, acct_id);
            util.DML(trFrame6_2);

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
            util.DML(trFrame6_3);

            String trFrame6_4 = String.format(
                    "SELECT ca_bal " +
                            "FROM customer_account " +
                            "WHERE ca_id = %s", acct_id);
            util.QUERY(trFrame6_4);
            ////////////
            util.TCL("commit");
        }catch (Exception e){
            e.printStackTrace();
            util.TCL("rollbsck");
            return;
        }
        long endTime = System.currentTimeMillis();
        hStats.insertTime(4, endTime-startTime);
        hStats.increment(4);
    }
}
