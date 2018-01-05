import com.oceanai.util.HttpClientUtil;

/**
 * Created by WangRupeng on 2017/12/15.
 */
public class StaticStringTest {
    public static void main(String[] args) {
        System.out.println(HttpClientUtil.getUri());
        HttpClientUtil.setURI("192.168.1.104");
        System.out.println(HttpClientUtil.getUri());
        StaticTest staticTest = new StaticTest();
        staticTest.test();
    }
}
