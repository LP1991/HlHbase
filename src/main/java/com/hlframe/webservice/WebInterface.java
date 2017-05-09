package com.hlframe.webservice;

import javax.jws.WebParam;
import javax.jws.WebService;

/**
 * @类名: WebInterface
 * @职责说明:
 * @创建者: Primo
 * @创建时间: 2017/4/13
 */
@WebService
public interface WebInterface {
    String sayHello(@WebParam(name="userName")String userName);
    String getStaffs(@WebParam(name = "time") int time);
}
