package net.es.maddash.collector;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import net.es.maddash.checks.Check;
import net.es.maddash.checks.CheckHandler;
import net.es.maddash.collector.jobs.PollingJob;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class CollectorGlobals {
    static private Logger log = Logger.getLogger(CollectorGlobals.class);
    static private CollectorGlobals instance = null;
    static private String configFile = null;
    
    private Scheduler scheduler;
    private Semaphore jobQueueSemaphore;
    private PollingJob pollingJob;
    private int jobBatchSize;
    private int threadPoolSize;
    private long jobPollingInterval;
    private List<String> jobSources; 
    private List<String> resultStores;
    private String sslKeystore;
    private String sslKeystorePassword;
    
    private Map<String, CheckHandler> handlerMap;
    
    final private String PROP_JOB_BATCH_SIZE = "jobBatchSize";
    final private String PROP_JOB_THREAD_POOL_SIZE = "jobThreadPoolSize";
    final private String PROP_JOB_POLLING_INTERVAL = "jobPollingInterval";
    final private String PROP_JOB_SOURCES = "jobSources";
    final private String PROP_RESULT_STORES = "resultStores";
    final private String PROP_HANDLERS = "handlers";
    final private String PROP_HANDLERS_TYPE = "type";
    final private String PROP_HANDLERS_CLASS = "class";
    final private String PROP_HANDLERS_TIMEOUT = "timeout";
    final private String PROP_HANDLERS_PARAMS = "params";
    final private String PROP_SSL_KEYSTORE = "sslKeystore";
    final private String PROP_SSL_KEYSTORE_PASSWORD = "sslKeystorePassword";
    
    
    final static private int DEFAULT_JOB_BATCH_SIZE = 250;
    final static private int DEFAULT_THREAD_POOL_SIZE = 50;
    final static private int DEFAULT_JOB_POLLING_INTERVAL = 60000;
    final static private String DEFAULT_SSL_KEYSTORE = null;
    final static private String DEFAULT_SSL_KEYSTORE_PASSWORD = null;
    
    /**
     * Sets the configuration file to use
     * 
     * @param newConfigFile the configuration file to use on initialization
     */
    public static void init(String newConfigFile){
        configFile = newConfigFile;
    }
    
    private CollectorGlobals(){
      //check config file
        if(configFile == null){
            throw new RuntimeException("No config file set.");
        }
        Map config = null;
        try {
            config = (Map) Yaml.load(new File(configFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
        
        //get job batch size
        this.jobBatchSize = DEFAULT_JOB_BATCH_SIZE;
        if(config.containsKey(PROP_JOB_BATCH_SIZE) && config.get(PROP_JOB_BATCH_SIZE) != null){
            this.jobBatchSize = (Integer) config.get(PROP_JOB_BATCH_SIZE);
        }
        log.debug("jobBatchSize is " + this.jobBatchSize);
        
        //initialize semaphore
        this.jobQueueSemaphore = new Semaphore(this.jobBatchSize, true);
        
        //get thread pool size
        this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        if(config.containsKey(PROP_JOB_THREAD_POOL_SIZE) && config.get(PROP_JOB_THREAD_POOL_SIZE) != null){
            this.threadPoolSize = (Integer) config.get(PROP_JOB_THREAD_POOL_SIZE);
        }
        log.debug("threadPoolSize is " + this.threadPoolSize);
        
        //get job polling interval
        this.jobPollingInterval = DEFAULT_JOB_POLLING_INTERVAL;
        if(config.containsKey(PROP_JOB_POLLING_INTERVAL) && config.get(PROP_JOB_POLLING_INTERVAL) != null){
            this.jobPollingInterval = ((Long) config.get(PROP_JOB_POLLING_INTERVAL)) * 1000L;
        }
        log.debug("jobPollingInterval is " + this.jobBatchSize);
        
        
        //get keystore information
        this.sslKeystore = DEFAULT_SSL_KEYSTORE;
        if(config.containsKey(PROP_SSL_KEYSTORE) && config.get(PROP_SSL_KEYSTORE) != null){
            this.sslKeystore = ((String) config.get(PROP_SSL_KEYSTORE));
        }
        log.debug("sslKeystore is " + this.sslKeystore);
        
        this.sslKeystorePassword = DEFAULT_SSL_KEYSTORE_PASSWORD;
        if(config.containsKey(PROP_SSL_KEYSTORE_PASSWORD) && config.get(PROP_SSL_KEYSTORE_PASSWORD) != null){
            this.sslKeystorePassword = ((String) config.get(PROP_SSL_KEYSTORE_PASSWORD));
        }
        log.debug("sslKeystorePassword is " + this.sslKeystorePassword);
        
        //get job sources
        if(!config.containsKey(PROP_JOB_SOURCES) || config.get(PROP_JOB_SOURCES) == null){
            throw new RuntimeException("Configuration Error: The propert jobSources must be set");
        }
        this.jobSources = (List<String>) config.get(PROP_JOB_SOURCES);
        
        //get result stores
        if(!config.containsKey(PROP_RESULT_STORES) || config.get(PROP_RESULT_STORES) == null){
            throw new RuntimeException("Configuration Error: The propert resultStores must be set");
        }
        this.resultStores = (List<String>) config.get(PROP_RESULT_STORES);
        
        //get check handlers
        if(!config.containsKey(PROP_HANDLERS) || config.get(PROP_HANDLERS) == null){
            throw new RuntimeException("Configuration Error: No handlers configured. " +
            		"Please add a handlers section to your configuration file.");
        }
        List<Map> handlerList = (List<Map>) config.get(PROP_HANDLERS);
        this.handlerMap = new HashMap<String,CheckHandler>();
        for(Map handlerPropMap : handlerList){
            CheckHandler handler = new CheckHandler();
            
            //Check that type is valid
            if(!handlerPropMap.containsKey(PROP_HANDLERS_TYPE) || handlerPropMap.get(PROP_HANDLERS_TYPE) == null){
                throw new RuntimeException("Configuration error: Found handler missing 'type' property");
            }
            if(this.handlerMap.containsKey(handlerPropMap.get(PROP_HANDLERS_TYPE)) && 
                    this.handlerMap.get(handlerPropMap.get(PROP_HANDLERS_TYPE)) != null){
                throw new RuntimeException("Configuration error: Found multiple handlers for type " + 
                        handlerPropMap.get(PROP_HANDLERS_TYPE));
            }
            handler.setType((String)handlerPropMap.get(PROP_HANDLERS_TYPE));
            
            if(!handlerPropMap.containsKey(PROP_HANDLERS_TIMEOUT) || handlerPropMap.get(PROP_HANDLERS_TIMEOUT) == null){
                throw new RuntimeException("Configuration error: Found handler missing 'timeout' property");
            }
            handler.setTimeout((Integer)handlerPropMap.get(PROP_HANDLERS_TIMEOUT));
            
            //check that class is valid
            if(!handlerPropMap.containsKey(PROP_HANDLERS_CLASS) || handlerPropMap.get(PROP_HANDLERS_CLASS) == null){
                throw new RuntimeException("Configuration error: Found handler missing 'class' property");
            }
            try {
                handler.setCheckClass(this.loadClass((String)handlerPropMap.get(PROP_HANDLERS_CLASS)));
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to load class " + 
                        handlerPropMap.get(PROP_HANDLERS_CLASS) + ": " + e.getMessage());
            }
            
            //finally if any parameters then set them
            if(handlerPropMap.containsKey(PROP_HANDLERS_PARAMS) || handlerPropMap.get(PROP_HANDLERS_PARAMS) == null){
                handler.setParams((Map)handlerPropMap.get(PROP_HANDLERS_PARAMS));
            }
            
            this.handlerMap.put(handler.getType(), handler);
        }
        
        //initialize scheduler threads
        Properties props = new Properties();
        props.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
        props.setProperty("org.quartz.threadPool.threadCount", this.threadPoolSize + "");
        try{
            SchedulerFactory schedFactory = new StdSchedulerFactory(props);
            this.scheduler = schedFactory.getScheduler();
            this.scheduler.start();
        }catch(Exception e){
            throw new RuntimeException(e.getMessage());
        }
        //job that checks for new jobs is in own thread
        this.pollingJob = new PollingJob("MaDDashCollectorPollingJob", this.jobPollingInterval);
        this.pollingJob.start();
    }
    
    synchronized static public CollectorGlobals getInstance() {
        if(instance == null){
            instance = new CollectorGlobals();
        }

        return instance;
    }
    
    private Class loadClass(String className) throws ClassNotFoundException {
        ClassLoader classLoader = CollectorGlobals.class.getClassLoader();
        Class checkClass = classLoader.loadClass(className);
        for(Class iface : checkClass.getInterfaces()){
            if(iface.getName().equals(Check.class.getName())){
                return checkClass;
            }
        }
        throw new RuntimeException("Class " + className + " does not implement Check interface");
    }

    /**
     * @return the scheduler
     */
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    /**
     * @param scheduler the scheduler to set
     */
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * @return the jobBatchSize
     */
    public int getJobBatchSize() {
        return this.jobBatchSize;
    }

    /**
     * @param jobBatchSize the jobBatchSize to set
     */
    public void setJobBatchSize(int jobBatchSize) {
        this.jobBatchSize = jobBatchSize;
    }

    /**
     * @return the threadPoolSize
     */
    public int getThreadPoolSize() {
        return this.threadPoolSize;
    }

    /**
     * @param threadPoolSize the threadPoolSize to set
     */
    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    /**
     * @return the jobPollingInterval
     */
    public long getJobPollingInterval() {
        return this.jobPollingInterval;
    }

    /**
     * @param jobPollingInterval the jobPollingInterval to set
     */
    public void setJobPollingInterval(long jobPollingInterval) {
        this.jobPollingInterval = jobPollingInterval;
    }

    /**
     * @return the jobSources
     */
    public List<String> getJobSources() {
        return this.jobSources;
    }

    /**
     * @param jobSources the jobSources to set
     */
    public void setJobSources(List<String> jobSources) {
        this.jobSources = jobSources;
    }

    /**
     * @return the sslKeystore
     */
    public String getSslKeystore() {
        return this.sslKeystore;
    }

    /**
     * @param sslKeystore the sslKeystore to set
     */
    public void setSslKeystore(String sslKeystore) {
        this.sslKeystore = sslKeystore;
    }

    /**
     * @return the sslKeystorePassword
     */
    public String getSslKeystorePassword() {
        return this.sslKeystorePassword;
    }

    /**
     * @param sslKeystorePassword the sslKeystorePassword to set
     */
    public void setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    /**
     * @return the handlerMap
     */
    public Map<String, CheckHandler> getHandlerMap() {
        return this.handlerMap;
    }

    /**
     * @param handlerMap the handlerMap to set
     */
    public void setHandlerMap(Map<String, CheckHandler> handlerMap) {
        this.handlerMap = handlerMap;
    }

    /**
     * @return the resultStores
     */
    public List<String> getResultStores() {
        return this.resultStores;
    }

    /**
     * @return the jobQueueSemaphore
     */
    public Semaphore getJobQueueSemaphore() {
        return this.jobQueueSemaphore;
    }
}
