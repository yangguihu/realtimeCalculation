package com.gome.bigData.bi.db.mysql;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by MaLi on 2017/1/17.
 */
public class TestQueryRunner {
    public static void test01() {
        GomeQueryRunner queryRunner = new GomeQueryRunner();
        java.lang.String contr_nbr = "20170118016601000001";
        Long merchandise_order_id = (Long) queryRunner.queryObject("ccsdb", "select merchandise_order_id from ccs_merchandise_order where contr_nbr=?", "merchandise_order_id", contr_nbr);
        System.out.println(merchandise_order_id);
    }

    public static void test02() {
        GomeQueryRunner queryRunner = new GomeQueryRunner();
        //Timestamp passport = (Timestamp) queryRunner.queryObject("passport", "select register_date from t_user where customer_id=?", "register_date", "23455efd-4ea6-40f7-aa4c-86e9d763f758");
        Timestamp passport = (Timestamp) queryRunner.queryObject("passport", "select register_date from t_user where customer_id=?", "register_date", "4c05a29c-6e1f-43db-85f2-ec37b1877d78");
        //Date date = new Date(passport);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String format1 = format.format(passport);
        System.err.println(format1);

    }

    public static void main(String[] args) {
        test02();
    }
}
