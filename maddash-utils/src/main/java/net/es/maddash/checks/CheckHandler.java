package net.es.maddash.checks;

import java.util.Map;

public class CheckHandler {
    protected String type;
    protected Class checkClass;
    protected int timeout;
    protected Map params;
    
    
    /**
     * @return the type
     */
    public String getType() {
        return this.type;
    }
    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }
    /**
     * @return the checkClass
     */
    public Class getCheckClass() {
        return this.checkClass;
    }
    /**
     * @param checkClass the checkClass to set
     */
    public void setCheckClass(Class checkClass) {
        this.checkClass = checkClass;
    }
    /**
     * @return the timeout
     */
    public int getTimeout() {
        return this.timeout;
    }
    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    /**
     * @return the params
     */
    public Map getParams() {
        return this.params;
    }
    /**
     * @param params the params to set
     */
    public void setParams(Map params) {
        this.params = params;
    }
}
