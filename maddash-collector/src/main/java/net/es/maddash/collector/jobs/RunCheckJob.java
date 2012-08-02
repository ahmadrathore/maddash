package net.es.maddash.collector.jobs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.es.maddash.checks.Check;
import net.es.maddash.checks.CheckConstants;
import net.es.maddash.checks.CheckHandler;
import net.es.maddash.checks.CheckResult;
import net.es.maddash.collector.CollectorGlobals;
import net.es.maddash.collector.http.JobsClient;
import net.es.maddash.logging.NetLogger;
import net.es.maddash.time.ISO8601Util;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class RunCheckJob implements Job{
    private Logger log = Logger.getLogger(RunCheckJob.class);
    private Logger netlogger = Logger.getLogger("netlogger");
    
    final private String JSON_PARAM_JOB_ID = "id";
    final private String JSON_PARAM_JOB_SERVICE_ID = "service-id";
    final private String JSON_PARAM_JOB_PARAMETERS = "parameters";
    
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try{
            this.doExecute(context);
        }catch(Exception e){
            this.log.error("Error executing job: " + e.getMessage());
        }finally{
            CollectorGlobals.getInstance().getJobQueueSemaphore().release();
        }
    }
    
    private void doExecute(JobExecutionContext context) throws JobExecutionException {
        NetLogger netLog = NetLogger.getTlogger();
        netlogger.debug(netLog.start("maddash.collector.RunCheckJob.execute"));
        
        //load jobdatamap
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        CheckHandler handler = (CheckHandler) dataMap.get("handler");
        JobsClient jobsCLient = (JobsClient) dataMap.get("jobsClient");
        List<String> resultUrls = (List<String>) dataMap.get("resultURLs");
        JSONObject jsonJob = (JSONObject)dataMap.get("jsonJob");
        JSONObject jsonCheckParams = new JSONObject();
        if(jsonJob.containsKey(JSON_PARAM_JOB_PARAMETERS) && 
                jsonJob.get(JSON_PARAM_JOB_PARAMETERS) != null){
            jsonCheckParams = (JSONObject) jsonJob.get(JSON_PARAM_JOB_PARAMETERS);
        }
        
        //load check
        Check checkToRun = null;
        try {
            checkToRun = (Check)handler.getCheckClass().newInstance();
        } catch (Exception e) {
            log.error("Error loading check: " + e.getMessage());
            e.printStackTrace();
            return;
        }
        
        //run check
        HashMap<String,String> netLogFields = new HashMap<String,String>();
        netLogFields.put("checkClass", handler.getCheckClass() + "");
        netLogFields.put("checkType", handler.getType());
        
        CheckResult result = null;
        try{
            netlogger.info(netLog.start("maddash.collector.RunCheckJob.execute.runCheck", null, null, netLogFields));
            Map preparedParams = checkToRun.prepareFromJSON(jsonCheckParams, handler.getParams());
            result = checkToRun.check(preparedParams, handler.getTimeout());
            netLogFields.put("resultCode", result.getResultCode()+"");
            netLogFields.put("resultMsg", result.getMessage());
            netlogger.info(netLog.end("maddash.collector.RunCheckJob.execute.runCheck", null, null, netLogFields));
            log.debug("Result code is " + result.getResultCode());
            log.debug("Result msg is " + result.getMessage());
        }catch(Exception e){
            result = new CheckResult(CheckConstants.RESULT_UNKNOWN, e.getMessage(), null);
            netlogger.error(netLog.end("maddash.collector.RunCheckJob.execute.runCheck", e.getMessage(), null, netLogFields));
            log.error("Error running check: " + e.getMessage());
            e.printStackTrace();
        }
        
        try {
            netlogger.debug(netLog.start("maddash.collector.RunCheckJob.execute.postResult"));
            //build JSON
            JSONObject resultJSON = new JSONObject();
            resultJSON.put("job-id", jsonJob.get(JSON_PARAM_JOB_ID));
            resultJSON.put("service-id", jsonJob.get(JSON_PARAM_JOB_SERVICE_ID));
            resultJSON.put("status", result.getResultCode());
            resultJSON.put("message", result.getMessage());
            resultJSON.put("time", ISO8601Util.fromTimestamp(System.currentTimeMillis()));
            resultJSON.put("parameters", result.getStats());
            for(String resultUrl: resultUrls){
                jobsCLient.postResult(resultUrl, resultJSON);
            }
            //send JSON
            netlogger.debug(netLog.end("maddash.collector.RunCheckJob.execute.postResult"));
        } catch (Exception e) {
            netlogger.debug(netLog.error("maddash.collector.RunCheckJob.execute.postResult", e.getMessage()));
            e.printStackTrace();
        }
        
        netlogger.debug(netLog.end("maddash.collector.RunCheckJob.execute"));
    }
}
