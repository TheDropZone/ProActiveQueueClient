package Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.ecs.AmazonECSClient;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

public class AwsConfig {

    private AwsConfigurationValues values;

    public AwsConfig(){

    }

    public AwsConfig(AwsConfigurationValues values){
        setConfiguration(values);
    }

    public AwsConfigurationValues getConfiguration(){
        if(values == null){
            throw new IllegalStateException("You must provide an AwsConfigurationValues config to setConfiguration()");
        }
        return this.values;
    }

    public void setConfiguration(AwsConfigurationValues values){
        this.values = values;
    }

    private AWSCredentialsProvider getCredentials(){
        if(values == null){
            throw new IllegalStateException("You must provide an AwsConfigurationValues config to setConfiguration()");
        }
        final String access = values.awsAccessKey;
        final String secret = values.awsAccessKey;
        return new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        return access;
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return secret;
                    }
                };
            }
            @Override
            public void refresh() {

            }
        };
    }

    /**
     * Gets an aws ECS client, with  the region specified in the config. Use the alternative method to choose a different region.
     * @return
     */
    public AmazonECSClient getECSClient(){
        String region = getConfiguration().getAwsRegion();
        if(region == null){
            throw new IllegalStateException("You must provide the awsRegion in the OrchestrationConfig.AwsConfigurationValues");
        }
        return this.getECSClient(region);
    }

    /**
     * Gets an aws ECS client, with a provided aws region with the same format as 'us-east-2'
     * @return
     */
    public AmazonECSClient getECSClient( String awsRegion){
        return (AmazonECSClient) AmazonECSClient.builder()
                .withRegion(awsRegion)
                .withCredentials(getCredentials())
                .build();
    }

    public AmazonCloudWatch getCloudWatchClient(){
        String region = getConfiguration().getAwsRegion();
        if(region == null){
            throw new IllegalStateException("You must provide the awsRegion in the OrchestrationConfig.AwsConfigurationValues");
        }
        return this.getCloudWatchClient(region);
    }

    public AmazonCloudWatch getCloudWatchClient(String awsRegion){
        return AmazonCloudWatchClientBuilder.standard()
                .withRegion(awsRegion)
                .withCredentials(getCredentials())
                .build();
    }


    @Data
    @EqualsAndHashCode
    @NoArgsConstructor
    public class AwsConfigurationValues{
        private String awsAccessKey;
        private String awsSecretKey;
        private String awsActiveMQNamespace = "AWS/AmazonMQ";
        private String awsActiveMQBrokerName;
        private String awsRegion = "us-east-2";
        private List<String> ecsSecurityGroups;
        private List<String> ecsVpcSubnets;
    }
}
