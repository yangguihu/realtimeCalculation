package com.gome.bigData.bi.topologys.fen;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by MaLi on 2017/1/3.
 */
public class MessageScheme implements Scheme {

    @Override
    public List<Object> deserialize(byte[] ser) {
        String msg = null;
        try {
            msg = new String(ser, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new Values(msg);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
