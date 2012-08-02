package net.es.maddash.collector.http;

import java.net.URI;

import net.es.maddash.logging.NetLogger;
import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.log4j.Logger;

public class JobsClient {
    private Logger log = Logger.getLogger(JobsClient.class);
    private Logger netLogger = Logger.getLogger("netLogger");
    Protocol httpsProtocol;


    public JobsClient(String keystore, String keystorePassword){
        if(keystore == null){
            return;
        }
        /*
         * Set https handler when client created. This allows program to 
         * use specific stores without affecting JVM-wide environment
         */
        this.httpsProtocol =  new Protocol("https", 
                new CustomSSLSocketFactory(keystore, keystorePassword), 443);
    }

    public JSONObject getList(String url) {
        NetLogger netLog = NetLogger.getTlogger();
        JSONObject json = null;

        //callback URI
        this.netLogger.info(netLog.start("maddash.collector.http.JobsClient.getList", null, url));
        try {
            HttpClient client = new HttpClient();
            URI uri = new URI(url);
            GetMethod getMethod = null;
            if(this.httpsProtocol != null && this.httpsProtocol.getScheme().equals(uri.getScheme())){
                client.getHostConfiguration().setHost(uri.getHost(), 
                        (uri.getPort() < 0 ? this.httpsProtocol.getDefaultPort() : uri.getPort()), 
                        this.httpsProtocol);
                getMethod =  new GetMethod(uri.getPath());
            }else{
                getMethod = new GetMethod(url);
            }

            getMethod.setFollowRedirects(true);
            client.executeMethod(getMethod);

            json = JSONObject.fromObject(getMethod.getResponseBodyAsString());

            this.netLogger.info(netLog.end("maddash.collector.http.JobsClient.getList", null, url));
        } catch (Exception e) {
            e.printStackTrace();
            this.log.error("Error contacting " + url + ": " + e.getMessage());
            this.netLogger.info(netLog.error("maddash.collector.http.JobsClient.getList", e.getMessage(), url));
        }

        return json;
    }
    
    public void postResult(String url, JSONObject result) {
        NetLogger netLog = NetLogger.getTlogger();

        //callback URI
        this.netLogger.info(netLog.start("maddash.collector.http.JobsClient.postResult", null, url));
        try {
            HttpClient client = new HttpClient();
            URI uri = new URI(url);
            PostMethod postMethod = null;
            if(this.httpsProtocol != null && this.httpsProtocol.getScheme().equals(uri.getScheme())){
                client.getHostConfiguration().setHost(uri.getHost(), 
                        (uri.getPort() < 0 ? this.httpsProtocol.getDefaultPort() : uri.getPort()), 
                        this.httpsProtocol);
                postMethod =  new PostMethod(uri.getPath());
            }else{
                postMethod = new PostMethod(url);
            }
            this.log.debug("Posting result: " + result);
            StringRequestEntity requestEntity = new StringRequestEntity(result+"", "application/json", "UTF-8");
            postMethod.setRequestEntity(requestEntity);
            client.executeMethod(postMethod);
            this.log.debug("Result post returned: " + postMethod.getResponseBodyAsString());
            this.netLogger.info(netLog.end("maddash.collector.http.JobsClient.postResult", null, url));
        } catch (Exception e) {
            e.printStackTrace();
            this.log.error("Error contacting " + url + ": " + e.getMessage());
            this.netLogger.info(netLog.error("maddash.collector.http.JobsClient.postResult", e.getMessage(), url));
        }
    }

}
