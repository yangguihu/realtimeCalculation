package com.gome.bigData.bi.db.relationdb.impl;

import com.gome.bigData.bi.db.relationdb.BaseDaoImpl;
import com.gome.bigData.bi.domain.Index_fen;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.SQLException;

/**
 * Created by MaLi on 2017/1/9.
 */
public class DaoImpl_fen extends BaseDaoImpl<Index_fen> {

    @Override
    public void save(Index_fen index_fen) throws SQLException {
        String sql = "insert into realtime_fen (" +
                "time_stamps," +
                "user_register_curday," +
                "user_auth_curday," +
                "user_regauth_curday," +
                "user_intopiece_curday," +
                "user_regintopiece_curday," +
                "user_syspass_curday," +
                "user_regsyspass_curday," +
                "user_telpass_curday," +
                "user_regtelpass_curday," +
                "user_loanamount_curday," +
                "user_regloanamount_curday," +
                "loanamount_curday) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        runner.update(sql,
                index_fen.getTime_stamps(),
                index_fen.getUser_register_curday(),
                index_fen.getUser_auth_curday(),
                index_fen.getUser_regauth_curday(),
                index_fen.getUser_intopiece_curday(),
                index_fen.getUser_regintopiece_curday(),
                index_fen.getUser_syspass_curday(),
                index_fen.getUser_regsyspass_curday(),
                index_fen.getUser_telpass_curday(),
                index_fen.getUser_regtelpass_curday(),
                index_fen.getUser_loanamount_curday(),
                index_fen.getUser_regloanamount_curday(),
                index_fen.getLoanamount_curday());
    }

    @Override
    public void delete(Long id) throws SQLException {
        String sql = "delete from realtime_fen where time_stamp=?";
        runner.update(sql,id);
    }

    @Override
    public void update(Index_fen index_fen) throws SQLException {
        String sql = "update realtime_fen  set (" +
                "time_stamps," +
                "user_register_curday," +
                "user_auth_curday," +
                "user_regauth_curday," +
                "user_intopiece_curday," +
                "user_regintopiece_curday," +
                "user_syspass_curday," +
                "user_regsyspass_curday," +
                "user_telpass_curday," +
                "user_regtelpass_curday," +
                "user_loanamount_curday," +
                "user_regloanamount_curday," +
                "loanamount_curday) values (?,?,?,?,?,?,?,?,?,?,?,?) where time_stamp=?";
        runner.update(sql,
                index_fen.getUser_register_curday(),
                index_fen.getUser_auth_curday(),
                index_fen.getUser_regauth_curday(),
                index_fen.getUser_intopiece_curday(),
                index_fen.getUser_regintopiece_curday(),
                index_fen.getUser_syspass_curday(),
                index_fen.getUser_regsyspass_curday(),
                index_fen.getUser_telpass_curday(),
                index_fen.getUser_regtelpass_curday(),
                index_fen.getUser_loanamount_curday(),
                index_fen.getUser_regloanamount_curday(),
                index_fen.getLoanamount_curday(),
                index_fen.getTime_stamps());
    }

    @Override
    public Index_fen query(Long id) throws SQLException {
        String sql = "select (" +
                "time_stamps," +
                "user_register_curday," +
                "user_auth_curday," +
                "user_regauth_curday," +
                "user_intopiece_curday," +
                "user_regintopiece_curday," +
                "user_syspass_curday," +
                "user_regsyspass_curday," +
                "user_telpass_curday," +
                "user_regtelpass_curday," +
                "user_loanamount_curday," +
                "user_regloanamount_curday," +
                "loanamount_curday) from realtime_fen where time_stamp=?";
        return runner.query(sql, new ScalarHandler<Index_fen>());
    }
}
