package com.gome.bigData.bi.tools;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by MaLi on 2017/2/3.
 */
public class DateTools {
    /**
     * 判断传入的日期是否是当天日期
     * @param toBeChecked
     * @return
     */
    public static boolean isToday(String toBeChecked){
        Date today = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String today_fomatted = simpleDateFormat.format(today);
        if(toBeChecked!=null&&toBeChecked.startsWith(today_fomatted)){
            return true;
        }else{
            return false;
        }
    }

    /**
     *转换时间戳类型到字符串
     * @param date 需要转换为字符串的SQL时间戳
     * @return
     */
    public static String getStringDate(Timestamp date){
        if(date!=null) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            return format.format(date);
        }
        return null;
    }

    /**
     * 判断两个字符串是否为同一天
     * @param date1
     * @param date2
     * @return
     */
    public static boolean isSameDate(String date1,String date2){
        boolean flag = false;
        if(date1!=null&&date2!=null&&date1.startsWith(date2)){
            flag=true;
        }
        return flag;
    }
}
