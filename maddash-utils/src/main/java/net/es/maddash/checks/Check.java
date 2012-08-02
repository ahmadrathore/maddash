package net.es.maddash.checks;

import java.util.Map;

import net.sf.json.JSONObject;

/**
 * Interface that each check called by the scheduler must implement
 * @author Andy Lake<andy@es.net>
 *
 */
public interface Check {
    /**
     * Called by the scheduler to prepare check when scheduler is remote
     * 
     * @param jsonParams a jsonObject of parameters to process
     * @param configParams processing instructuoins for json params
     * @return the result of the preparations
     */
    public Map prepareFromJSON(JSONObject jsonParams, Map configParams);
    
    /**
     * Called by the scheduler to prepare check when scheduler is local
     * 
     * @param gridName the name of the grid containing this check 
     * @param rowName the name of the row containing this check
     * @param colName the name of the column containing this check
     * @param params a map of check-specific parameters defined in the check configuration
     * @return the result of the preparations
     */
    public Map prepareFromYAML(String gridName, String rowName, String colName, Map params);
    
    /**
     * Called by the scheduler when it is time to run the check
     * 
     * @param params a map of check-specific parameters defined in the check configuration
     * @param timeout the maximum amount of time to wait for this check
     * @return the result of the check
     */
    public CheckResult check(Map params, int timeout);
}
