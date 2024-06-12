import java.io.IOException;

public class SocialMediaApp {
    public static void main(String[] args) {
        Thread consumerThread = new Thread(() -> {
        	SocialMediaRedisConsumer.main(new String[]{});
        });

        Thread producerThread = new Thread(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            SocialMediaProducter.main(new String[]{});
        });

        consumerThread.start();
        producerThread.start();
    }
}