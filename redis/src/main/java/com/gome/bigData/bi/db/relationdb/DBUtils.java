package com.gome.bigData.bi.db.relationdb;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;

/**
 * Created by MaLi on 2017/1/9.
 */
public class DBUtils {
    private static ComboPooledDataSource ds;
    static{
        try {
            ds = new ComboPooledDataSource("xxx");//查找指定的数据库连接配置
            //ds = new ComboPooledDataSource();//查找默认的数据库连接配置
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    public static DataSource getDataSource(){
        return ds;
    }
}
