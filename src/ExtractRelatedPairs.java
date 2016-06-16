import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

public class ExtractRelatedPairs {

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
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar")
                .withMainClass("Phase1")
//                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data/", "hdfs:///output1/");
                .withArgs("s3n://dsps162assignment2benasaf/input/", "hdfs:///output1/");



        StepConfig step1Config = new StepConfig()
                .withName("Phase 1")
                .withHadoopJarStep(jarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar")
                .withMainClass("Phase2")
                .withArgs("hdfs:///output1/", "hdfs:///output2/");

        StepConfig step2Config = new StepConfig()
                .withName("Phase 2")
                .withHadoopJarStep(jarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar")
                .withMainClass("Phase3")
                .withArgs("hdfs:///output2/", "hdfs:///output3/");

        StepConfig step3Config = new StepConfig()
                .withName("Phase 3")
                .withHadoopJarStep(jarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://dsps162assignment2benasaf/jars/ExtractRelatedPairs.jar")
                .withMainClass("Phase4")
                .withArgs("hdfs:///output3/", "s3n://dsps162assignment2benasaf/output/", args[0]);

        StepConfig step4Config = new StepConfig()
                .withName("Phase 4")
                .withHadoopJarStep(jarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(15)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
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

        System.out.println("Submitting the JobFlow Request to Amazon EMR and running it...");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}