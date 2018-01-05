import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Created by WangRupeng on 2017/11/15.
 */
public class FloatArrayTest {
    public static void main(String[] args) {
        float[] center = new float[5];
        System.out.println(Arrays.toString(center));
        for (float v : center) {
            v = 1;
        }

        System.out.println(Arrays.toString(center));
    }
}
