import java.awt.image.BufferedImage;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by WangRupeng on 2017/12/2.
 */
public class QueueTest {
    public static void main(String[] args) {
        LinkedBlockingQueue<BufferedImage> frameQueue2 = new LinkedBlockingQueue<>();
        try {
            BufferedImage bufferedImage = frameQueue2.poll();
            System.out.println(bufferedImage.getHeight());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
