package com.gome.bigData.bi.db.mysql;

import backtype.storm.tuple.Values;
import com.gome.bigData.bi.db.relationdb.BaseDaoImpl;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by MaLi on 2017/1/12.
 */
public class GomeQueryRunner implements Serializable{

    public void save(String dbName,String sql){

    }

    /**
     * 查询指定字段id的值
     * @param dbName
     * @param sql
     * @param id
     * @param tClass
     * @param params
     * @return
     */
    public Object query(String dbName, String sql, String id, Class tClass, String ...params){
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String result = null;
        Object t =null;

        try {
            connection = DBUtils.getDataSource(dbName).getConnection();
            stmt = connection.prepareStatement(sql);
            for (int i=0;i<params.length;i++){
                stmt.setString(i+1,params[i]);
            }
            rs = stmt.executeQuery();
            t = tClass.newInstance();
            while (rs.next()) {
                result = rs.getString(id);
                t = rs.getObject(id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtils.release(connection, stmt, rs);
        }
        return t;
    }

    /**
     *
     * @param dbName
     * @param sql
     * @param id 要查询的字段
     * @param params 参数
     * @return
     */
    public Object queryObject(String dbName, String sql, String id,Object ...params){
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Object t =null;
        try {
            connection = DBUtils.getDataSource(dbName).getConnection();

            stmt = connection.prepareStatement(sql);
            for (int i=0;i<params.length;i++){
                stmt.setObject(i+1,params[i]);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                t = rs.getObject(id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtils.release(connection, stmt, rs);
        }
        return t;
    }
    /**
     * 查询指定id是否存在于目标数据库中
     * @param dbName 数据库名,根据数据库名自动获取数据库连接
     * @param sql
     * @param id 要提取的"字段"
     * @param params 限制条件参数参数
     * @return
     */
    public Boolean exists(String dbName, String sql, String id, String ...params){
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String result = null;
        try {
            connection = DBUtils.getDataSource(dbName).getConnection();
            stmt = connection.prepareStatement(sql);
            for (int i=0;i<params.length;i++){
                stmt.setString(i+1,params[i]);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                result = rs.getString(id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBUtils.release(connection, stmt, rs);
        }
        return result != null;
    }
}
