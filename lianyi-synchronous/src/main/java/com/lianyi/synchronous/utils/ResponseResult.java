package com.lianyi.synchronous.utils;

/**
 * Created by Stu on 2018/7/26.
 */
public class ResponseResult {
    private String msg;
    private Integer code;
    private Object data;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
