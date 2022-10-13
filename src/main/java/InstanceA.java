import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InstanceA {
    public static void main(String[] args) {
        String bucket_name = "njit-cs-643";
        String key_name = "1.jpg";
        String queue_name  = "dheeraj_queue.fifo";
        System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

        DetectLabelsRequest request = new DetectLabelsRequest();
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                .withRegion("us-east-1")
                .build();
        //creating standard queue
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        CreateQueueRequest create_request = new CreateQueueRequest(queue_name)
                .addAttributesEntry("DelaySeconds", "60")
                .addAttributesEntry("MessageRetentionPeriod", "86400");
        CreateQueueRequest createStandardQueueRequest = new CreateQueueRequest("dheeraj_queue");
        String standardQueueUrl = sqs.createQueue(createStandardQueueRequest).getQueueUrl();
        //creating a fifo queue
        Map<String, String> queueAttributes = new HashMap<>();
        queueAttributes.put("FifoQueue", "true");
        queueAttributes.put("ContentBasedDeduplication", "true");
        CreateQueueRequest createFifoQueueRequest = new CreateQueueRequest(
                "dheeraj_queue.fifo").withAttributes(queueAttributes);
        String fifoQueueUrl = sqs.createQueue(createFifoQueueRequest)
                .getQueueUrl();

        try {
            for(int i=1;i<11;i++){
                String imageName = i + ".jpg";
                com.amazonaws.services.s3.model.S3Object o = s3.getObject(bucket_name, imageName);
                S3ObjectInputStream s3is = o.getObjectContent();
                //below is the code to download s3 bucket images locally on my system
               //FileOutputStream fos = new FileOutputStream(new File(imageName));
                //byte[] read_buf = new byte[1024];
                //int read_len = 0;
                //while ((read_len = s3is.read(read_buf)) > 0) {
                 //   fos.write(read_buf, 0, read_len);
                //}
                //s3is.close();
                //fos.close();
                request.withImage(new Image().withS3Object(new com.amazonaws.services.rekognition.model.S3Object().withName(imageName).withBucket(bucket_name)));
                request.withMaxLabels(10);
                request.withMinConfidence(90F);
                try {
                    DetectLabelsResult LabelResult = rekognitionClient.detectLabels(request);
                    List<Label> labels = LabelResult.getLabels();
                    for (Label CurrentLabel : labels) {
                        if(CurrentLabel.getName().equals("Car") && CurrentLabel.getConfidence() >90){
                            System.out.println("Car detected at label " + o.getKey());
                            System.out.println("Label: " + CurrentLabel.getName());
                            System.out.println("Confidence: " + CurrentLabel.getConfidence().toString() + "\n");

                            SendMessageRequest send_msg_request = new SendMessageRequest()
                                    .withQueueUrl(fifoQueueUrl)
                                    .withMessageGroupId("queue-group-image")
                                    .withMessageBody(imageName);

                            sqs.sendMessage(send_msg_request);

                        }
                    }
                } catch (AmazonRekognitionException e) {
                    e.printStackTrace();
                }
            }

        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }

    }
}