import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class InstanceB {

    static Boolean endOfQueue=false;
    static String queueUrl;
    static String label;

    public static void main(String[] Args) {
        String bucket_name = "njit-cs-643";
        String queue_name = "dheeraj_queue.fifo";
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                .withRegion("us-east-1")
                .build();

        ListQueuesResult lq_result = sqs.listQueues();
        for (String url : lq_result.getQueueUrls()) {
            if (url.contains(queue_name)) {
                queueUrl = url;
                break;
            }
        }
        HashMap<String, String> map = new HashMap<>();
        while (!endOfQueue) {

            List<Message> messages = sqs.receiveMessage(queueUrl).getMessages();
            if (messages.size() > 0) {
                Message message = messages.get(0);
                label = message.getBody();
                if (label.equals("-1")) {
                    endOfQueue = true;
                } else {
                    System.out.println("Processing the image " + label);
                    DetectTextRequest request = new DetectTextRequest()
                            .withImage(new Image().withS3Object(new com.amazonaws.services.rekognition.model.S3Object().withName(label).withBucket(bucket_name)));
                    try {
                        DetectTextResult result = rekognitionClient.detectText(request);
                        List<TextDetection> textDetections = result.getTextDetections();

                        if (textDetections.size() != 0) {
                            String text = "";
                            text = textDetections.get(0).getDetectedText();
                            System.out.println(text);
                            map.put(label, text);
                        }
                        //System.out.println("This is statement after iteration of if:" );
                        // Delete the message in the queue now because it has been processed
                        for (Message msg : messages) {
                            sqs.deleteMessage(queueUrl, msg.getReceiptHandle());
                        }
                    } catch (Exception e) {
                        System.err.println(e.getLocalizedMessage());
                        System.exit(1);
                    }
                }

            }
        }
        try {
            FileWriter writer = new FileWriter("output.txt");
            Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> pair = it.next();
                writer.write(pair.getKey() + ":" + pair.getValue() + "\n");
                it.remove();
            }
            writer.close();
            System.out.println("Results written to the file output.txt");
        } catch (IOException e) {
            System.out.println("An error occurred writing to the file.");
            e.printStackTrace();
        }

    }
}
