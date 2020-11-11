package com.ProActiveQueue.ProActiveQueueClient.Orchestration;

import com.ProActiveQueue.ProActiveQueueClient.Configuration.AwsConfig;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.*;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EcsControllerService {

    private final Logger log = LoggerFactory.getLogger(EcsControllerService.class);

    private AmazonECSClient ecsClient;

    private String queueName;

    private String clusterName;

    private String serviceName;

    private String taskName;

    private ScheduledExecutorService scheduler;

    private AwsConfig config;

    private MetricsService metrics;

    private Integer MESSAGES_PER_TASK = 10;
    private Integer MAX_TASKS = 50;
    private Integer MIN_TASKS = 1;
    private Integer repeatPeriod = 1;
    private TimeUnit repeatPeriodUnit = TimeUnit.SECONDS;

    private AtomicBoolean processing = new AtomicBoolean(false);

    public EcsControllerService(String clusterName,
                                String serviceName,
                                String taskName,
                                String queueName,
                                AwsConfig config){
        this(clusterName,serviceName,taskName,queueName,config.getECSClient(),config.getCloudWatchClient(),config);
    }

    public EcsControllerService(String clusterName,
                                String serviceName,
                                String taskName,
                                String queueName,
                                AmazonECSClient ecsClient,
                                AmazonCloudWatch cloudwatch,
                                AwsConfig config){
        this.clusterName = clusterName;
        this.serviceName = serviceName;
        this.taskName = taskName;
        this.queueName = queueName;
        this.ecsClient = ecsClient;
        this.config = config;
        this.metrics = new MetricsService(clusterName,serviceName,taskName,queueName,ecsClient,cloudwatch,config);
    }

    public void startController(){
        startController(MIN_TASKS, MAX_TASKS, MESSAGES_PER_TASK);
    }

    public void startController(Integer minimumTaskCount, Integer maximumTaskCount, Integer messagesPerTask){
        startController(minimumTaskCount,maximumTaskCount,messagesPerTask,null,null);
    }

    public void startController(Integer minimumTaskCount, Integer maximumTaskCount, Integer messagesPerTask, Integer repeatPeriod, TimeUnit repeatPeriodUnit){
        this.MIN_TASKS = minimumTaskCount;
        this.MAX_TASKS = maximumTaskCount;
        this.MESSAGES_PER_TASK = messagesPerTask;

        this.scheduler = Executors.newScheduledThreadPool(1);
        this.repeatPeriod = (repeatPeriod != null) ? repeatPeriod : this.repeatPeriod;
        this.repeatPeriodUnit = (repeatPeriodUnit != null) ? repeatPeriodUnit : this.repeatPeriodUnit;

        metrics.setOnUpdate(() -> {
            if(!processing.get()){
                processing.set(true);
                try {
                    setRunningTaskCount(calculateDesiredTaskCount());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    processing.set(false);
                }
            }
        });

        this.scheduler.scheduleAtFixedRate(() -> {
            this.metrics.loadMetrics();
        },0,this.repeatPeriod, this.repeatPeriodUnit);

    }

    private Integer calculateDesiredTaskCount(){
        Double requests = metrics.getRequestsInQueue();
        Double tasks = metrics.getRunningTasks();
        Integer desiredTaskCount = Double.valueOf(Math.ceil(requests / MESSAGES_PER_TASK)).intValue();
        return Ints.constrainToRange(desiredTaskCount,MIN_TASKS,MAX_TASKS);
    }

    private void setRunningTaskCount(Integer desiredCount) throws InterruptedException {
        Integer currentTasks = Math.max(metrics.getRunningTasks().intValue(),1); //there is a service that will always maintain 1 task (even if it says 0)
        Integer newTaskCount = Math.max(0,desiredCount - currentTasks);
        List<Task> newTasks = new ArrayList<>();
        List<Failure> failures = new ArrayList<>();
        final Integer newTaskCountFinal = newTaskCount;
        if(newTaskCount > 0){
            log.info("ECS Cluster: preparing to launch " + newTaskCountFinal + " new tasks");
            while(newTaskCount != 0){
                RunTaskRequest runTaskRequest = new RunTaskRequest();
                runTaskRequest.setCluster(clusterName);
                runTaskRequest.setLaunchType("FARGATE");
                NetworkConfiguration networkConfig = new NetworkConfiguration();
                AwsVpcConfiguration vpcConfig = new AwsVpcConfiguration();
                vpcConfig.setSecurityGroups(Optional.ofNullable(config.getConfiguration().getEcsSecurityGroups())
                        .orElseThrow(() -> new IllegalStateException("You must provide the ecsSecurityGroups in the OrchestrationConfig.AwsConfigurationValues config")));
                vpcConfig.setSubnets(Optional.ofNullable(config.getConfiguration().getEcsVpcSubnets())
                        .orElseThrow(() -> new IllegalStateException("You must provide the ecsVpcSubnets in the OrchestrationConfig.AwsConfigurationValues config")));
                vpcConfig.setAssignPublicIp("ENABLED");
                networkConfig.setAwsvpcConfiguration(vpcConfig);
                runTaskRequest.setNetworkConfiguration(networkConfig);
                runTaskRequest.setTaskDefinition(taskName);
                runTaskRequest.setCount(Integer.min(newTaskCount,10));
                try{
                    RunTaskResult runResult = ecsClient.runTask(runTaskRequest);
                    newTasks.addAll(runResult.getTasks());
                    failures.addAll(runResult.getFailures());
                    Integer successfulCount = Optional.ofNullable(runResult.getTasks()).map(tasks -> tasks.size()).orElse(0);
                    log.info("ECS Cluster: spun up " + successfulCount + " new tasks with a target of " + newTaskCountFinal + " new tasks total");
                    newTaskCount -= successfulCount;
                }catch(Exception e){
                    log.error(e.getMessage());
                    Thread.sleep(1000);
                }
                Thread.sleep(1000);
            }
            long start = System.currentTimeMillis();

            //Wait for cluster to reach desired count or 5 minutes to elapse
            Integer currentTaskCount = metrics.getNumberOfTasksInCluster().intValue();
            while(currentTaskCount < desiredCount && (System.currentTimeMillis() - start) < (1000*60*5)){
                log.info("Cluster Task Count: " + currentTaskCount + " | Target task count: " + desiredCount);
                Thread.sleep(500);
            }
        }
    }
}
