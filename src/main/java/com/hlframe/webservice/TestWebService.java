package com.hlframe.webservice;

import com.hlframe.utils.StaffCreator;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.w3c.dom.Element;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.xml.transform.Source;
import javax.xml.ws.Binding;
import javax.xml.ws.Endpoint;
import javax.xml.ws.EndpointReference;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;

/**
 * @类名: TestWebService
 * @职责说明:
 * @创建者: Primo
 * @创建时间: 2017/4/13
 */
@WebService(endpointInterface="com.hlframe.webservice.WebInterface", serviceName="helloWorldService")
public class TestWebService implements WebInterface{

    @WebMethod
    public String sayHello(String userName) {
        System.out.println("HelloWorldServiceImp.sayHello("+userName+")");
        return "hi, my name is "+userName;
    }



    @WebMethod
    public String getStaffs(int time){
        Random r = new Random();
        JSONObject result = new JSONObject();
        int count = 500 +r.nextInt(500);
        JSONArray staffs = new JSONArray();
        try {
            for (int i=0;i<count;i++){
                staffs.put(StaffCreator.createStaff());
            }
            result.put("total",staffs.length());
            result.put("data",staffs);
            result.put("time",time);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return result.toString();
    }

}
