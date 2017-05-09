package com.hlframe.action;

import com.hlframe.utils.ConfigUtils;
import net.neoremind.sshxcute.core.*;
import net.neoremind.sshxcute.task.*;
import net.neoremind.sshxcute.task.impl.ExecCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;

/**
 * @类名: MainAction
 * @职责说明: receive the request to link hbase
 * @创建者: Primo
 * @创建时间: 2017/4/6
 */
@Path("/mainaction")
public class MainAction {
    private static String quorum = ConfigUtils.getProperty("hbase.zookeeper.quorum");
    private static final Logger logger = LoggerFactory.getLogger(MainAction.class);
    //创建配置文件
    static Configuration config = HBaseConfiguration.create();
    //配置zookeeper信息
    static {
        config.set("hbase.zookeeper.quorum", quorum);
    }

    /**
     * 服务健康情况测试
     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String testServer() {
        return "healthy";
    }
    /**
     * @name: createTable
     * @funciton: create Hbase table
     * @param data
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/createTable")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public String createTable(@FormParam("data") String data) {
        HBaseAdmin admin = null;
        try {
            //新建admin对象用于连接hbase
            admin = new HBaseAdmin(config);

            JSONObject a = new JSONObject(data);//接收传入字符串，转换为jasonobject
            String tableName = a.get("tableName").toString();

            //判断table 是否已经存在如果存在 设置disable并删除
            if(admin.isTableAvailable(tableName)){
                if(admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }
            //新建table 对象
            TableName tableName1 = TableName.valueOf(tableName);
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName1);
//            tableDescriptor.set
            //新建列族 cf1
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf1");
            columnDescriptor.setBlockCacheEnabled(true);
            columnDescriptor.setInMemory(true);
            columnDescriptor.setMaxVersions(10);
            tableDescriptor.addFamily(columnDescriptor);
            //创建表
            admin.createTable(tableDescriptor);
            logger.info("create table success!");
        }catch (Exception e){
            e.printStackTrace();
            logger.error("create table fail!",e);
        }finally {
            //关闭资源
            closeAdmin(admin);
        }
        return null;
    }

    /**
     * @name: deleteTable
     * @funciton: delete Hbase table
     * @param data
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/deleteTable")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public String deleteTable(@FormParam("data") String data) {

        HBaseAdmin admin = null;
        try {
            //新建admin对象用于连接hbase
            admin = new HBaseAdmin(config);

            JSONObject a = new JSONObject(data);//接收传入字符串，转换为jasonobject
            String tableName = a.get("tableName").toString();

            //判断table 是否已经存在如果存在 设置disable并删除
            if(admin.isTableAvailable(tableName)){
                if(admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }
            logger.info("delete table success!");
        }catch (Exception e){
            e.printStackTrace();
            logger.error("delete table fail!",e);
        }finally {
            //关闭资源
            closeAdmin(admin);
        }
        return null;
    }


    /**
     * @name: put
     * @funciton: insert into hbase table
     * @param data
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/put")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public void put(@FormParam("data") JSONObject data) throws Exception{

        HTable table = null;
        try {
//            JSONObject a = new JSONObject(data);//接收传入字符串，转换为jasonobject
            String tableName = data.get("tableName").toString();
            //新建HTable对象用于连接hbase
            table = new HTable(config,tableName);
            //解析json对象
            Put put = transJson2Put(data);
            //添加到table
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
            logger.error("insert row to hbase fail! data:"+data,e);
        }finally {
            //关闭资源
            closeTable(table);
        }
    }

    /**
     * @name: delete
     * @funciton: delete
     * @param data
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/delete")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public void delete(@FormParam("data") JSONObject data) throws Exception{
        HTable table = null;
        try {
            String tableName = data.get("tableName").toString();
            //新建HTable对象用于连接hbase
            table = new HTable(config,tableName);
            //创建delete 对象
            String rowKey = data.get("rowKey").toString();
            Delete delete = new Delete(rowKey.getBytes());
            //删除
            table.delete(delete);
            logger.info("delete "+rowKey+" success!");
        }catch (Exception e){
            e.printStackTrace();
            logger.error("delete row from hbase fail! data:"+data,e);
        }finally {
            //关闭资源
            closeTable(table);
        }
    }
    /**
     * @name: delete
     * @funciton: delete the rows
     * @param tableName
     * @param datas delete datas
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/deleteAll")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public void deleteAll(@FormParam("tableName") String tableName,@FormParam("data") JSONArray datas) throws Exception{
        if (null==datas || datas.length() == 0){
            return;
        }

        HTable table = null;
        try {
            table = new HTable(config,tableName);
            List<Delete> deletes = new ArrayList<>();
            for (int i =0;i<datas.length();i++){
                JSONObject data = new JSONObject(datas.get(i).toString());
                //创建delete 对象
                String rowKey = data.get("rowKey").toString();
                Delete delete = new Delete(rowKey.getBytes());
                deletes.add(delete);
            }
            table.delete(deletes);
        }catch (Exception e){
            e.printStackTrace();
            logger.error("delete rows from hbase fail! data:"+datas,e);
        }finally {
            //关闭资源
            closeTable(table);
        }
    }

    /**
     * @name: put
     * @funciton: insert into hbase table
     * @param tableName
     * @param datas
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/puts")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public void put(@FormParam("tableName") String tableName,@FormParam("data") JSONArray datas) throws Exception{
        if (null==datas || datas.length() == 0){
            return;
        }

        HTable table = null;
        try {
            table = new HTable(config,tableName);
            List<Put> puts = new ArrayList<>();
            for (int i =0;i<datas.length();i++){
                JSONObject data = new JSONObject(datas.get(i).toString());
                //解析json对象
                Put put = transJson2Put(data);
                puts.add(put);
            }
            table.put(puts);
        }catch (Exception e){
            e.printStackTrace();
            logger.error("insert row to hbase fail! data:"+datas,e);
        }finally {
            //关闭资源
            closeTable(table);
        }
    }

    /**
     * @name: get
     * @funciton: get row value by rowkey
     * @param data
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/get")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public String get(@FormParam("data") JSONObject data) throws Exception{
        JSONObject result = new JSONObject();
//        JSONObject json = new JSONObject(data);
        HTable table = null;
        try {
            String tableName = data.get("tableName").toString();
            //新建HTable对象用于连接hbase
            table = new HTable(config,tableName);
            Get get = transJson2Get(data);
            Result res = table.get(get);

            for (Cell cell : res.listCells()){
                //cell 内的值复制到json
                cellValue2Json(cell,result);
            }
            return result.toString();
        }catch (Exception e){
            e.printStackTrace();
            logger.error("get row from hbase fail! data:"+data,e);
        }finally {
            //关闭资源
            closeTable(table);
        }
        return null;
    }

    /**
     * @name: scan
     * @funciton: scan row value by startRow and lastRow
     * @param data
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/scan")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public String scan(@FormParam("data") JSONObject data) throws Exception{
        JSONArray return_results = new JSONArray();
        HTable table = null;
        try {
            String tableName = data.get("tableName").toString();
            //新建HTable对象用于连接hbase
            table = new HTable(config,tableName);

            //rowkey 按照字典排序
            Scan scan = new Scan();
            //设置起始row和终止row
            scan.setStartRow(data.get("startRow").toString().getBytes());
            scan.setStopRow(data.get("stopRow").toString().getBytes());
            //查询
            ResultScanner results = table.getScanner(scan);

            //遍历results
            for (Result res : results){
                JSONObject obj = new JSONObject();
                for (Cell cell : res.listCells()){
                    cellValue2Json(cell,obj);
                }
                return_results.put(obj);
            }
            return return_results.toString();
        }catch (Exception e){
            e.printStackTrace();
            logger.error("get row from hbase fail! data:"+data,e);
        }finally {
            //关闭资源
            closeTable(table);
        }
        return null;
    }

    /**
     * @name: scan&filter
     * @funciton: scan row value by startRow and lastRow
     * @param data
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/scanAndfilter")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public String scanAndfilter(JSONObject data) throws Exception{

        JSONArray return_results = new JSONArray();
        HTable table = null;
        try {
            String tableName = data.get("tableName").toString();
            //新建HTable对象用于连接hbase
            table = new HTable(config,tableName);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            //TODO: filter different field.
            PrefixFilter prefixFilter = new PrefixFilter("shoujihao".getBytes());
            filterList.addFilter(prefixFilter);
            SingleColumnValueFilter valueFilter = new SingleColumnValueFilter("cd1".getBytes(),"type".getBytes(),
                    CompareFilter.CompareOp.EQUAL,"0".getBytes());
            filterList.addFilter(valueFilter);
            //rowkey 按照字典排序
            Scan scan = new Scan();
            //设置起始row和终止row
            scan.setFilter(filterList);
            scan.setStartRow(data.get("startRow").toString().getBytes());
            scan.setStopRow(data.get("lastRow").toString().getBytes());
            //查询
            ResultScanner results = table.getScanner(scan);
            //遍历results
            for (Result res : results){
                JSONObject obj = new JSONObject();
                for (Cell c : res.listCells()){
                    System.out.println(new String(c.getValue()));
                    obj.put(new String(c.getValueArray()),new String(c.getFamilyArray())+new String(c.getQualifierArray()));
                }
                return_results.put(obj);
            }
            return return_results.toString();
        }catch (Exception e){
            e.printStackTrace();
            logger.error("get row from hbase fail! data:"+data,e);
        }finally {
            //关闭资源
            closeTable(table);
        }
        return null;
    }

    /**
     * @name: getCount
     * @funciton: getTotolCount
     * @param tableName
     * @return
     * @Create by lp at 2017/4/5 15:13
     * @throws Exception
     */
    @POST
    @Path("/getTotolCount")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public String getTotolCount(String tableName){
        net.neoremind.sshxcute.core.Result res = null;
        //创建shell 脚本
        String sqoopScript = "sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.RowCounter '"+tableName+"'";;
        System.out.println("sqoopScript:  "+sqoopScript);
        //创建ssh连接实例
        ConnBean cb = new ConnBean(ConfigUtils.getProperty("hbase.client.address"),
                ConfigUtils.getProperty("hbase.client.loginUser", "root"),
                ConfigUtils.getProperty("hbase.client.loginPswd"));
        SSHExec ssh = SSHExec.getInstance(cb);
        //准备源路径与备份路径
        try {
            // 连接到hdfs
            ssh.connect();
            //执行采集脚本
            net.neoremind.sshxcute.task.CustomTask ct1 =new ExecCommand(sqoopScript);
                    res = ssh.exec(ct1);
            //返回的永远是错误标记... 根据返回错误信息 手工判断是否执行成功
            if(checkResultErr(res)){
                System.out.println("--------------------success");
                return resolveCount(res.error_msg);
            }else{
                //采集失败  还原数据(全量采集 模式下)
                System.out.println("--------------------------fail");
                return "0";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(null!=ssh){
                ssh.disconnect();
            }
        }
        return "0";
    }

    /**
     * @name:  close
     * @funciton:
     * @param table
     * @return
     * @Create by lp at 2017/4/10 19:29
     * @throws
     */
    private void closeTable(HTable table) {
        if (table != null){
            try {
                table.close();
            } catch (IOException e) {
                logger.error("close table fail!",e);
                e.printStackTrace();
            }
        }
    }

    /**
     * @name: transJson2Get
     * @funciton: transfor json to get
     * @param data
     * @return
     * @Create by lp at 2017/4/8 11:01
     * @throws
     */
    private Get transJson2Get(JSONObject data) throws JSONException {
        String rowKey = data.get("rowKey").toString();
        Get get = new Get(rowKey.getBytes());
        //删除多余数据
        data.remove("rowKey");
        data.remove("tableName");
        Iterator<String> keys = data.keys();
        while(keys.hasNext()){
            String key = keys.next();
            //key 结构为columnFamily:qualifier, 解析结构
            String[] familyAndQualifier = key.split(":");
            if(familyAndQualifier.length == 2){
                get.addColumn(familyAndQualifier[0].getBytes(),familyAndQualifier[1].getBytes());
            }
        }
        return get;
    }

    /**
     * @name: transJson2Put
     * @funciton: transfor json to put
     * @param data jsonObj
     * @return Put Obj
     * @Create by lp at 2017/4/8 10:16
     * @throws
     */
    private Put transJson2Put(JSONObject data) throws JSONException {
        String rowKey = data.get("rowKey").toString();
        Put put = new Put(rowKey.getBytes());
        //删除多余数据
        data.remove("rowKey");
        data.remove("tableName");
        Iterator<String> keys = data.keys();
        while(keys.hasNext()){
            String key = keys.next();
            //key 结构为columnFamily:qualifier, 解析结构
            String[] familyAndQualifier = key.split(":");
            if(familyAndQualifier.length == 2){
                put.addColumn(familyAndQualifier[0].getBytes(),familyAndQualifier[1].getBytes(),data.get(key).toString().getBytes());
            }
        }
        return put;
    }
    /**
     * @name: cellValue2Json
     * @funciton: change cell value to json Obj
     * @param
     * @return
     * @Create by lp at 2017/4/10 19:41
     * @throws
     */
    private void cellValue2Json(Cell cell, JSONObject obj) throws JSONException {
        String key = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+
                "_"+Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
        String value =
                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        System.out.println(key+": "+value);
        obj.put(key,value);
    }

    /**
     * @name: closeAdmin
     * @funciton: closeAdmin
     * @param
     * @return
     * @Create by lp at 2017/4/10 19:42
     * @throws
     */
    private void closeAdmin(HBaseAdmin admin) {
        if (admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                logger.error("close admin fail!",e);
                e.printStackTrace();
            }
        }
    }

    /**
     * @name: resolveCount
     * @funciton: 解析shell返回的字段找到Rows
     * @param
     * @return
     * @Create by lp at 2017/4/12 17:30
     * @throws
     */
    private String resolveCount(String error_msg) {
        String result = error_msg;
        int start = result.indexOf("ROWS=");
        int end = result.indexOf("\t",start);
        return result.substring(start+5,end);
    }

    private boolean checkResultErr(net.neoremind.sshxcute.core.Result res) {
        System.out.println("==============================="+res.error_msg);
        if(res.isSuccess){
            return true;
        }
        //手工解析error信息 判断日志中是否存在'completed successfully'
        return res.error_msg.indexOf("completed successfully")>0;
    }

    public static void main(String[] args){
        MainAction action = new MainAction();
        try {
//            action.put("t_user");
//            JSONArray arr = new JSONArray();
//Random r = new Random();
//            for (int i=1;i<=1000;i++){
//                JSONObject obj = new JSONObject();
//                obj.put("rowKey","1865872711014914813079"+r.nextInt(1000));
//                obj.put("cf1:time", "123456"+r.nextInt(1000));
//                obj.put("cf1:type", "24"+r.nextInt(1000));
//                arr.put(obj);
//            }
//
//            action.put("t_user",arr);
//
//            JSONObject o = new JSONObject();
//            o.put("startRow","1865872711014914813079100");
//            o.put("stopRow","1865872711014914813079110");
//            o.put("tableName","t_user");
//            String s = action.scan(o);
//            arr = new JSONArray(s);
//
//            System.out.println(arr.length());
//            action.createTable(obj.toString());
//            action.createTable(obj.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
