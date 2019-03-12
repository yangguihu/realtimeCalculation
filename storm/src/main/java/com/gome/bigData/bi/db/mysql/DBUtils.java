package com.gome.bigData.bi.db.mysql;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by MaLi on 2017/1/10.
 */
public class DBUtils {
    private static ComboPooledDataSource ds = null;//返回的数据库连接池
    private static ComboPooledDataSource ds_passport = null;
    private static ComboPooledDataSource ds_ccsdb= null;
    private static ComboPooledDataSource ds_rmpsdb_fen= null;
    private static ComboPooledDataSource canal_test= null;
    static{
        try {
            ds_passport = new ComboPooledDataSource("passport");//查找指定的数据库连接配置
            ds_ccsdb = new ComboPooledDataSource("ccsdb");//查找指定的数据库连接配置
            ds_rmpsdb_fen = new ComboPooledDataSource("rmpsdb_fen");//查找指定的数据库连接配置
            canal_test = new ComboPooledDataSource("canal_test");//查找指定的数据库连接配置
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    public static DataSource getDataSource(String db_name){
        switch (db_name){
            case "passport":
                ds =  ds_passport;
                break;
            case "ccsdb":
                ds =  ds_ccsdb;
                break;
            case "rmpsdb_fen":
                ds =  ds_rmpsdb_fen;
                break;
            case "canal_test":
                ds =  canal_test;
                break;
        }
        return ds;
    }

    public static void release(Connection conn, Statement stmt, ResultSet resultSet){
        if(resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                resultSet=null;
            }
        }
        if(stmt!=null){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                stmt=null;
            }
        }
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                conn=null;
            }
        }
    }
}
