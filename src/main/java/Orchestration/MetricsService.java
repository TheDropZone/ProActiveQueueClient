package Orchestration;

import Configuration.AwsConfig;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.ListTasksResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class MetricsService {

    private final Logger log = LoggerFactory.getLogger(MetricsService.class);

    private AmazonCloudWatch cloudwatch;

    private AmazonECSClient ecsClient;

    private AwsConfig config;

    private String queueName;

    private String clusterName;

    private String serviceName;

    private String taskName;

    private Double requests;

    private Double runningTasks;

    private Runnable onUpdate;

    public MetricsService(String clusterName, String serviceName, String taskName, String queueName, AwsConfig config){
        this(clusterName,serviceName,taskName,queueName, config.getECSClient(), config.getCloudWatchClient(), config);
    }

    public MetricsService(String clusterName, String serviceName, String taskName, String queueName, AmazonECSClient ecsClient, AmazonCloudWatch cloudwatch, AwsConfig config){
        this.clusterName = clusterName;
        this.serviceName = serviceName;
        this.taskName = taskName;
        this.queueName = queueName;
        this.cloudwatch = cloudwatch;
        this.ecsClient = ecsClient;
        this.config = config;
    }

    /**
     * This method is used to load metrics for the given esc cluster and queue
     */
    public void loadMetrics(){
        this.requests = getRequestsInQueue(queueName);
        this.runningTasks = getNumberOfTasksInCluster();
        log.info("AWS Metrics: (Requests in Queue) = " + requests + " : (Tasks in Cluster) = " + runningTasks);

        if(onUpdate != null){
            onUpdate.run();
        }
    }

    public void setOnUpdate(Runnable onUpdate){
        this.onUpdate = onUpdate;
    }

    public Double getRequestsInQueue(){
        return this.requests;
    }

    public Double getRunningTasks(){
        return this.runningTasks;
    }

    public Double getNumberOfTasksInCluster(){
        ListTasksRequest tasksRequest = new ListTasksRequest();
        tasksRequest.setCluster(clusterName);
        tasksRequest.setFamily(taskName);
        ListTasksResult tasks = ecsClient.listTasks(tasksRequest);

        if(tasks != null && tasks.getTaskArns() != null){
            return Integer.valueOf(tasks.getTaskArns().size()).doubleValue();
        }
        return null;
    }

    private Double getRequestsInQueue(String queueName){
        GetMetricDataRequest request = new GetMetricDataRequest();
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.MINUTE,-1);
        request.setStartTime(cal.getTime());
        request.setEndTime(new Date());
        List<MetricDataQuery> queries = new ArrayList<>();
        MetricDataQuery dataQuery = new MetricDataQuery();
        dataQuery.setId("q1");
        dataQuery.setReturnData(true);
        MetricStat stat = new MetricStat();
        stat.setPeriod(1);
        Metric met = new Metric();
        String awsActiveMQNamespace = config.getConfiguration().getAwsActiveMQNamespace();
        met.setNamespace(awsActiveMQNamespace);
        met.setMetricName("QueueSize");
        List<Dimension> dims = new ArrayList<>();
        Dimension dim = new Dimension();
        dim.setName("Broker");
        String awsActiveMQBrokerName = config.getConfiguration().getAwsActiveMQBrokerName();
        if(awsActiveMQBrokerName == null){
            throw new IllegalStateException("You must provide the AwsActiveMQBrokerName in the OrchestrationConfig.AwsConfigurationValues config");
        }
        dim.setValue(awsActiveMQBrokerName);
        dims.add(dim);
        Dimension dim2 = new Dimension();
        dim2.setName("Queue");
        dim2.setValue(queueName);
        dims.add(dim2);
        met.setDimensions(dims);
        stat.setMetric(met);
        stat.setStat("Average");
        dataQuery.setMetricStat(stat);
        queries.add(dataQuery);

        request.setMetricDataQueries(queries);
        GetMetricDataResult results = cloudwatch.getMetricData(request);
        List<Double> resultValues = results.getMetricDataResults().get(0).getValues();
        if(resultValues != null && !resultValues.isEmpty()){
            return resultValues.get(0);
        }else{
            return null;
        }
    }
}
