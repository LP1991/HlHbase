import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * @类名: Test
 * @职责说明:
 * @创建者: Primo
 * @创建时间: 2017/4/8
 */
public class Test {
    @org.junit.Test
    public void test1() throws JSONException {
        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("asd","asd");
        jsonObject.remove("sd");
        System.out.print(jsonObject);
    }
}
