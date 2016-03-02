package hih;

import java.util.Map;

/**
 * Created by mariosp on 8/2/16.
 */
public class quickDemo {

    static BenStatistics b = new BenStatistics();
    static hihUtil util = new hihUtil(b);

    public static void main(String args[]){

        util.CONNECT();
        util.DML("INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,                   t_s_symb, t_qty, " +
                "t_bid_price, t_ca_id,                   t_exec_name, t_trade_price, t_chrg, t_comm,                   " +
                "t_tax, t_lifo) VALUES (200000001000016, now(), 'PNDG', 'TLS', false, 'TRFXPRA',         " +
                "309, 26.28, 43000008322, 'Grant Pilloud Play Money', NULL, 10.60, 0.70, 0, false)");

        String trFrame1_1 = String.format(
                "SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg " +
                        "FROM trade " +
                        "WHERE t_id = 200000001000016");
        Map tr1_1 = util.QUERY2MAP(trFrame1_1);

        String acct_id 		= tr1_1.get("t_ca_id").toString();
        String type_id 		= tr1_1.get("t_tt_id").toString();
        String symbol 		= tr1_1.get("t_s_symb").toString();
        String trade_qty 	= tr1_1.get("t_qty").toString();
        String charge		= tr1_1.get("t_chrg").toString();

        util.DISCONNECT();


    }

}
/*
INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,                   t_s_symb, t_qty, t_bid_price, t_ca_id,                   t_exec_name, t_trade_price, t_chrg, t_comm,                   t_tax, t_lifo) VALUES (200000001000017, now(), 'PNDG', 'TLB', false, 'EPAXPRB',         537, 27.76, 43000009663, 'William Palmer Vacation Account', NULL, 10.60, 0.70, 0, true)
INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,                   t_s_symb, t_qty, t_bid_price, t_ca_id,                   t_exec_name, t_trade_price, t_chrg, t_comm,                   t_tax, t_lifo) VALUES (200000001000018, now(), 'PNDG', 'TLS', true, 'CMSE',         660, 23.78, 43000003992, 'Patricia Mouw Savings Account', NULL, 10.60, 0.70, 0, true)
INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,                   t_s_symb, t_qty, t_bid_price, t_ca_id,                   t_exec_name, t_trade_price, t_chrg, t_comm,                   t_tax, t_lifo) VALUES (200000001000019, now(), 'SBMT', 'TMS', false, 'AMFI',         512, 22.53, 43000007175, 'Bernard Tri Business Account', NULL, 10.60, 0.70, 0, false)
INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,                   t_s_symb, t_qty, t_bid_price, t_ca_id,                   t_exec_name, t_trade_price, t_chrg, t_comm,                   t_tax, t_lifo) VALUES (200000001000020, now(), 'SBMT', 'TMB', true, 'AN',         330, 25.73, 43000008095, 'Roselle Nelson Family Trust', NULL, 10.60, 0.70, 0, false)
INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,                   t_s_symb, t_qty, t_bid_price, t_ca_id,                   t_exec_name, t_trade_price, t_chrg, t_comm,                   t_tax, t_lifo) VALUES (200000001000021, now(), 'SBMT', 'TMS', true, 'VTAPRA',         647, 21.39, 43000006235, 'Joseph Warshauer Individual Account', NULL, 10.60, 0.70, 0, false)
INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,                   t_s_symb, t_qty, t_bid_price, t_ca_id,                   t_exec_name, t_trade_price, t_chrg, t_comm,                   t_tax, t_lifo) VALUES (200000001000022, now(), 'PNDG', 'TLS', true, 'LNT',         313, 29.78, 43000000761, 'John Clower House Money', NULL, 10.60, 0.70, 0, true)
 */