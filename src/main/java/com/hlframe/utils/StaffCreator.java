package com.hlframe.utils;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

/**
 * @类名: StaffCreator
 * @职责说明:
 * @创建者: Primo
 * @创建时间: 2017/4/14
 */
public class StaffCreator {
    private static Random r = new Random();

    public static JSONObject createStaff() throws JSONException {
        JSONObject staff = new JSONObject();
        staff.put("ID", UUID.randomUUID().toString().replaceAll("-",""));
        staff.put("gender",(byte)(r.nextInt(2)));
        staff.put("name",UUID.randomUUID().toString().substring(0,r.nextInt(10)+2));
        staff.put("age",r.nextInt(40)+18);
        setBirthday(staff);
        staff.put("remark",staff.toString());
        return staff;
    }

    private static void setBirthday(JSONObject staff) throws JSONException {
        int age = staff.getInt("age");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        calendar.set(calendar.get(Calendar.YEAR)-age,r.nextInt(12)+1,r.nextInt(31));
        staff.put("birthday",calendar.getTime());
        calendar.setTimeInMillis(System.currentTimeMillis());
        calendar.set(calendar.get(Calendar.YEAR)-r.nextInt(8)-1,r.nextInt(12)+1,r.nextInt(31));
        staff.put("createDate",calendar.getTime());
    }

}
