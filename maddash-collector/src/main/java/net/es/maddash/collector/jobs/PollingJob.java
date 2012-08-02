package net.es.maddash.collector.jobs;

import java.util.Date;
import java.util.UUID;

import net.es.maddash.collector.CollectorGlobals;
import net.es.maddash.collector.http.JobsClient;
import net.es.maddash.collector.jobs.PollingJob;
import net.es.maddash.logging.NetLogger;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;

public class PollingJob extends Thread{
    Logger log = Logger.getLogger(PollingJob.class);
    Logger netlogger = Logger.getLogger("netlogger");
    long sleepTime;
    
    final private String JSON_PARAM_JOBS = "jobs";
    final private String JSON_PARAM_JOB_TYPE = "type";
    
    public PollingJob(String name, long sleepTime){
        super(name);
        this.sleepTime = sleepTime;
    }
    
    public void run(){
        while(true){
            try{
                this.execute();
            }catch(Exception e){
                log.error("Error executing PollingJob: " + e.getMessage());
                e.printStackTrace();
            }finally{
                try {
                    Thread.sleep(this.sleepTime);
                } catch (InterruptedException e) {
                    log.error("Interrupt exception: " + e.getMessage());
                }
            }
        }
    }
    
    
    synchronized public void execute() {
        NetLogger netLog = NetLogger.getTlogger();
        netlogger.info(netLog.start("maddash.collector.CheckSchedulerJob.execute"));
        CollectorGlobals globals = CollectorGlobals.getInstance();
        
        //send rest request to get list of jobs
        JobsClient client = new JobsClient(globals.getSslKeystore(), globals.getSslKeystorePassword());
        for(String url : globals.getJobSources()){
            try{
                JSONObject jobsJSON = client.getList(url); 
                if(jobsJSON == null){
                    //if any errors then continue to next url
                    continue;
                }else if(!jobsJSON.containsKey(JSON_PARAM_JOBS)){
                    this.log.warn("JSON object with no 'jobs' element returned from " + url);
                }else if(jobsJSON.get(JSON_PARAM_JOBS) != null){
                    this.log.info("No jobs to run. Server returned null job list");
                }
                JSONArray jobArray = jobsJSON.getJSONArray(JSON_PARAM_JOBS);
                for(int i = 0; i < jobArray.size(); i++){
                    
                    JSONObject job = jobArray.getJSONObject(i);
                    
                    //check that it has a job type
                    if(!job.containsKey(JSON_PARAM_JOB_TYPE) || job.getString(JSON_PARAM_JOB_TYPE) == null){
                        this.log.error("Job returned with no job type");
                        continue;
                    }
                    
                    //find handler
                    if(!globals.getHandlerMap().containsKey(job.getString(JSON_PARAM_JOB_TYPE)) || 
                            globals.getHandlerMap().get(job.getString(JSON_PARAM_JOB_TYPE)) == null){
                        this.log.error("No handler for job type " + job.getString(JSON_PARAM_JOB_TYPE));
                        continue;
                    }
                    
                    //schedule job
                    String jobKey =  UUID.randomUUID().toString();
                    String triggerName = "runCheckTrigger-" + jobKey;
                    String jobName = "runCheckJob-" + jobKey;
                    SimpleTrigger trigger = new SimpleTrigger(triggerName, null, 
                            new Date(), null, 0, 0L);
                    JobDetail jobDetail = new JobDetail(jobName, "RUN_CHECKS",
                            RunCheckJob.class);
                    JobDataMap dataMap = new JobDataMap();
                    dataMap.put("handler", globals.getHandlerMap().get(job.getString(JSON_PARAM_JOB_TYPE)));
                    dataMap.put("jsonJob", job);
                    dataMap.put("jobsClient", client);
                    dataMap.put("resultURLs", globals.getResultStores());
                    jobDetail.setJobDataMap(dataMap);
                    //make sure we don't have a backlog of too many jobs
                    globals.getJobQueueSemaphore().acquire();
                    //schedule job
                    try{
                        globals.getScheduler().scheduleJob(jobDetail, trigger);
                    }catch(Exception e){
                        //make sure permit is released if scheduling error
                        globals.getJobQueueSemaphore().release();
                        throw e;
                    }
                }
            }catch(Exception e){
                this.log.error("Error occurred processing the" + url + " jobs list: " + e.getMessage());
            }
        }
        
        
        netlogger.info(netLog.end("maddash.collector.CheckSchedulerJob.execute"));
    }
}
