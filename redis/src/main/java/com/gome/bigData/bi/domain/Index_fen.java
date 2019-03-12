package com.gome.bigData.bi.domain;

import java.io.Serializable;

/**
 * Created by MaLi on 2017/1/9.
 */
public class Index_fen implements Serializable {
    private Long time_stamps;
    private Integer user_register_curday;//1,注册用户数
    private Integer user_auth_curday;//2,鉴权用户数
    private Integer user_regauth_curday;//3,当日注册并鉴权用户数
    private Integer user_intopiece_curday;//4,进件用户数
    private Integer user_regintopiece_curday;//5,当日注册并进件用户数
    private Integer user_syspass_curday;//6,系统通过用户数
    private Integer user_regsyspass_curday;//7,当日注册并系统通过用户数
    private Integer user_telpass_curday;//8,电核通过用户数
    private Integer user_regtelpass_curday;//9,当日注册并电核通过用户数
    private Integer user_loanamount_curday;//10,放款用户数
    private Integer user_regloanamount_curday;//11,当日注册并放款用户数
    private Double loanamount_curday;//12,放款金额

    public Long getTime_stamps() {
        return time_stamps;
    }

    public void setTime_stamps(Long time_stamps) {
        this.time_stamps = time_stamps;
    }

    public Integer getUser_register_curday() {
        return user_register_curday;
    }

    public void setUser_register_curday(Integer user_register_curday) {
        this.user_register_curday = user_register_curday;
    }

    public Integer getUser_auth_curday() {
        return user_auth_curday;
    }

    public void setUser_auth_curday(Integer user_auth_curday) {
        this.user_auth_curday = user_auth_curday;
    }

    public Integer getUser_regauth_curday() {
        return user_regauth_curday;
    }

    public void setUser_regauth_curday(Integer user_regauth_curday) {
        this.user_regauth_curday = user_regauth_curday;
    }

    public Integer getUser_intopiece_curday() {
        return user_intopiece_curday;
    }

    public void setUser_intopiece_curday(Integer user_intopiece_curday) {
        this.user_intopiece_curday = user_intopiece_curday;
    }

    public Integer getUser_regintopiece_curday() {
        return user_regintopiece_curday;
    }

    public void setUser_regintopiece_curday(Integer user_regintopiece_curday) {
        this.user_regintopiece_curday = user_regintopiece_curday;
    }

    public Integer getUser_syspass_curday() {
        return user_syspass_curday;
    }

    public void setUser_syspass_curday(Integer user_syspass_curday) {
        this.user_syspass_curday = user_syspass_curday;
    }

    public Integer getUser_regsyspass_curday() {
        return user_regsyspass_curday;
    }

    public void setUser_regsyspass_curday(Integer user_regsyspass_curday) {
        this.user_regsyspass_curday = user_regsyspass_curday;
    }

    public Integer getUser_telpass_curday() {
        return user_telpass_curday;
    }

    public void setUser_telpass_curday(Integer user_telpass_curday) {
        this.user_telpass_curday = user_telpass_curday;
    }

    public Integer getUser_regtelpass_curday() {
        return user_regtelpass_curday;
    }

    public void setUser_regtelpass_curday(Integer user_regtelpass_curday) {
        this.user_regtelpass_curday = user_regtelpass_curday;
    }

    public Integer getUser_loanamount_curday() {
        return user_loanamount_curday;
    }

    public void setUser_loanamount_curday(Integer user_loanamount_curday) {
        this.user_loanamount_curday = user_loanamount_curday;
    }

    public Integer getUser_regloanamount_curday() {
        return user_regloanamount_curday;
    }

    public void setUser_regloanamount_curday(Integer user_regloanamount_curday) {
        this.user_regloanamount_curday = user_regloanamount_curday;
    }

    public Double getLoanamount_curday() {
        return loanamount_curday;
    }

    public void setLoanamount_curday(Double loanamount_curday) {
        this.loanamount_curday = loanamount_curday;
    }
}
