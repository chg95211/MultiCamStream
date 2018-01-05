/**
 * Created by WangRupeng on 2017/11/29.
 */
public class SplitStringTest {
    public static void main(String[] args) {
        String text = "192.168.1.105*rtsp://admin:iec123456@192.168.1.59:554/h264/ch1/main/av_stream";

        String[] msg = text.split("\\*");
        if (msg.length == 2) {
            String ip = msg[0];
            String streamLocation = msg[1];
            System.out.println("IP is " + ip);
            System.out.println("Stream location is " + streamLocation);
        }
    }
}
