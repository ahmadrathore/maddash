package net.es.maddash.checks;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.es.maddash.logging.NetLogger;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

/**
 * Check implementation that runs a nagios command.It has one check specific parameter
 * named <i>command</i>. This is the nagios command to execute on the system. It loads the message
 * and statistics returned by the Nagios check in the result 
 * 
 * @author Andy Lake <andy@es.net>
 *
 */
public class NagiosCheck implements Check{
    private Logger log = Logger.getLogger(NagiosCheck.class);
    private Logger netlogger = Logger.getLogger("netlogger");
    
    final static public String PARAM_COMMAND = "command";
    final static public String PARAM_COMMANDOPTS = "commandOpts";
    final static public String PARAM_COMMANDOPTS_NAME = "name";
    final static public String PARAM_COMMANDOPTS_CMDOPT = "cmdOpt";
    final static public String PARAM_COMMANDOPTS_SWITCH = "switch";
    
    public Map prepareFromJSON(JSONObject jsonParams, Map configParams){
        String command = (String)configParams.get(PARAM_COMMAND);
        if(configParams.containsKey(PARAM_COMMANDOPTS) && 
                configParams.get(PARAM_COMMANDOPTS) != null){
            for(Map cmdOptMap : (List<Map>)configParams.get(PARAM_COMMANDOPTS)){
                //validate
                if(!cmdOptMap.containsKey(PARAM_COMMANDOPTS_NAME) || 
                        cmdOptMap.get(PARAM_COMMANDOPTS_NAME) == null){
                    throw new RuntimeException("Configuration exception: commandOpts missing 'name' parameter");
                }
                if(!cmdOptMap.containsKey(PARAM_COMMANDOPTS_CMDOPT) || 
                        cmdOptMap.get(PARAM_COMMANDOPTS_CMDOPT) == null){
                    throw new RuntimeException("Configuration exception: commandOpts missing 'cmdOpt' parameter");
                }
                //parse
                if(jsonParams.containsKey(cmdOptMap.get(PARAM_COMMANDOPTS_NAME)) && 
                        jsonParams.get(cmdOptMap.get(PARAM_COMMANDOPTS_NAME)) != null &&
                        cmdOptMap.containsKey(PARAM_COMMANDOPTS_SWITCH) &&
                        cmdOptMap.get(PARAM_COMMANDOPTS_SWITCH) != null &&
                        "1".equals(cmdOptMap.get(PARAM_COMMANDOPTS_SWITCH)+"")){
                    //if switch
                    String tmpVal = jsonParams.get(cmdOptMap.get(PARAM_COMMANDOPTS_NAME))+"";
                    command += ("1".equals(tmpVal.trim()) ? 
                                    " " +  cmdOptMap.get(PARAM_COMMANDOPTS_CMDOPT) : "");
                }else if(jsonParams.containsKey(cmdOptMap.get(PARAM_COMMANDOPTS_NAME)) && 
                        jsonParams.get(cmdOptMap.get(PARAM_COMMANDOPTS_NAME)) != null){
                    //if normal option
                    command += " " +  cmdOptMap.get(PARAM_COMMANDOPTS_CMDOPT);
                    command += " " +  jsonParams.get(cmdOptMap.get(PARAM_COMMANDOPTS_NAME));
                }
            }
        }

        HashMap<String, String> returnParams = new HashMap<String, String>();
        returnParams.put(PARAM_COMMAND, command);
        
        return returnParams;
    }
    
    public Map prepareFromYAML(String gridName, String rowName, String colName,
            Map params){
        if(!params.containsKey(PARAM_COMMAND) || params.get(PARAM_COMMAND) == null){
            throw new RuntimeException("No command parameter provided");
        }
        String command = (String)params.get(PARAM_COMMAND);
        command = command.replaceAll("%row", rowName);
        command = command.replaceAll("%col", colName);
        
        //load map
        HashMap<String, String> returnParams = new HashMap<String, String>();
        returnParams.put(PARAM_COMMAND, command);
        
        return returnParams;
    }
    
    public CheckResult check(Map params, int timeout) {
        if(!params.containsKey(PARAM_COMMAND) || params.get(PARAM_COMMAND) == null){
            return new CheckResult(CheckConstants.RESULT_UNKNOWN, 
                    "Command not defined. Please check config file", null);
        }
        String command = (String)params.get(PARAM_COMMAND);
        
        NetLogger netLog = NetLogger.getTlogger();
        CheckResult result = null;
        Runtime runtime = Runtime.getRuntime();
        Process process = null;
        HashMap<String,String> returnParams = new HashMap<String,String>();
        returnParams.put(PARAM_COMMAND, command);
        try {
            netlogger.debug(netLog.start("maddash.NagiosCheck.runCommand"));
            log.debug("Executing command " + command);
            process = runtime.exec(command);
            WatchDog watchdog = new WatchDog(process);
            watchdog.start();
            watchdog.join(timeout*1000);
            if(watchdog.exit == null){
                result = new CheckResult(CheckConstants.RESULT_TIMEOUT, 
                        "Command timed-out after " + timeout + " seconds",
                        returnParams);
            }else{
                int resultCode = process.exitValue();
                String outputLine = null;
                if(resultCode < CheckConstants.RESULT_SUCCESS || 
                        resultCode > CheckConstants.RESULT_UNKNOWN){
                    outputLine = "Unknown return status " + resultCode + " from command. Verify that it is a valid Nagios plug-in";
                    resultCode = CheckConstants.RESULT_UNKNOWN;
                }else{
                    BufferedReader stdin = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    outputLine = stdin.readLine();
                    returnParams = this.parseReturnParams(outputLine, returnParams);
                    outputLine = this.formatOutputLine(outputLine);
                }
                result = new CheckResult(resultCode, outputLine, returnParams);
            }
            netlogger.debug(netLog.end("maddash.NagiosCheck.runCommand"));
        } catch (Exception e) {
            netlogger.debug(netLog.error("maddash.NagiosCheck.runCommand", e.getMessage()));
            log.error("Error running nagios check: " + e.getMessage());
            result = new CheckResult(CheckConstants.RESULT_UNKNOWN, 
                    "Exception executing command: " + e.getMessage(), returnParams);
            e.printStackTrace();
        } finally{
            if(process != null){
                process.destroy();
            }
        }
        
        return result;
    }

    private String formatOutputLine(String outputLine) {
        outputLine = outputLine.replaceAll("^\\w+? \\w+? \\-", "");
        outputLine = outputLine.replaceAll("\\|.+", "");
        outputLine = outputLine.trim();
            
        return outputLine;
    }

    private HashMap<String,String> parseReturnParams(String outputLine, HashMap<String,String> params) {
        if(params == null){
            params = new HashMap<String, String>();
        }
        String[] lineParts = outputLine.split("\\|");
        if(lineParts.length == 1){
            return null;
        }
        
        String statsString = lineParts[lineParts.length - 1];
        String[] kvPairs = statsString.split(";+");
        for(String kvPair : kvPairs){
            kvPair = kvPair.trim();
            String[] kv = kvPair.split("=");
            if(kv.length == 2){
                params.put(kv[0].toLowerCase().trim(), kv[1].trim());
            }
        }
        
        return params;
    }

    private class WatchDog extends Thread{
        private Process process;
        private Integer exit = null;
        
        public WatchDog(Process process){
            this.process = process;
        }
        
        public void run(){
            try {
                this.exit = this.process.waitFor();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

}
