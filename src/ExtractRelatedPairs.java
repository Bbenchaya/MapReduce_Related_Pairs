import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileWriter;

public class ExtractRelatedPairs {

    private static String OUTPUT_SIZE_FILENAME = "outputSize.txt";

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: ExtractRelatedPairs: <k>");
            System.exit(1);
        }
        int outputSize = Integer.parseInt(args[0]);
        if (outputSize < 0) {
            System.err.println("k should be positive");
            System.exit(1);
        }

        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        try {
            System.out.print("Uploading output size description file to S3... ");
            File file = new File(OUTPUT_SIZE_FILENAME);
            FileWriter fw = new FileWriter(file);
            fw.write(args[0] + "\n");
            fw.flush();
            fw.close();
            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/resource", OUTPUT_SIZE_FILENAME, file));
            System.out.println("Done.");
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

        HadoopJarStepConfig jarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar") // This should be a full map reduce application.
                .withMainClass("Phase1")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data", "s3n://dsps162assignment2benasaf/output1/");

        StepConfig step1Config = new StepConfig()
                .withName("Phase 1")
                .withHadoopJarStep(jarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar") // This should be a full map reduce application.
                .withMainClass("Phase2")
                .withArgs("s3n://dsps162assignment2benasaf/output1/", "s3n://dsps162assignment2benasaf/output2/");

        StepConfig step2Config = new StepConfig()
                .withName("Phase 2")
                .withHadoopJarStep(jarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar") // This should be a full map reduce application.
                .withMainClass("Phase3")
                .withArgs("s3n://dsps162assignment2benasaf/output2/", "s3n://dsps162assignment2benasaf/output3/");

        StepConfig step3Config = new StepConfig()
                .withName("Phase 3")
                .withHadoopJarStep(jarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar") // This should be a full map reduce application.
                .withMainClass("Phase4")
                .withArgs("s3n://dsps162assignment2benasaf/output3/", "s3n://dsps162assignment2benasaf/output4/");

        StepConfig step4Config = new StepConfig()
                .withName("Phase 4")
                .withHadoopJarStep(jarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(20)
                .withMasterInstanceType(InstanceType.M1Small.toString())
                .withSlaveInstanceType(InstanceType.M1Small.toString())
                .withHadoopVersion("2.7.2")
                .withEc2KeyName("AWS")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("extract-related-word-pairs")
                .withInstances(instances)
                .withSteps(step1Config, step2Config, step3Config, step4Config)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withReleaseLabel("emr-4.7.0")
                .withLogUri("s3n://dsps162assignment2benasaf/logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}