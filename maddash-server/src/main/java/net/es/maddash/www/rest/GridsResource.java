package net.es.maddash.www.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;

import net.es.maddash.MaDDashGlobals;
import net.es.maddash.logging.NetLogger;
import net.sf.json.JSONObject;

@Path("/maddash/grids")
public class GridsResource {
    Logger log = Logger.getLogger(GridsResource.class);
    Logger netLogger = Logger.getLogger("netLogger");
    @Context UriInfo uriInfo;
    
    final private String GET_EVENT = "maddash.www.rest.GridsResource.get";
    
    @Produces("application/json")
    @GET
    public Response get(@Context HttpHeaders httpHeaders){
        NetLogger netLog = NetLogger.getTlogger();
        this.netLogger.info(netLog.start(GET_EVENT));
        
        JSONObject json = null;
        try{
            json = MaDDashGlobals.getInstance().getResourceManager().getGrids(uriInfo);
        }catch(Exception e){
            this.netLogger.error(netLog.error(GET_EVENT, e.getMessage()));
            return Response.serverError().entity(e.getMessage()).build();
        }
        
        this.netLogger.info(netLog.end(GET_EVENT));
        return Response.ok().entity(json.toString()).build();
    }
}
