package cn.poi.redis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.poi.Poi;

import com.alibaba.fastjson.JSON;

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.geo.GeoRadiusParam;

public class JedisClusterHelper {

    private static Logger logger = LoggerFactory.getLogger(JedisClusterHelper.class);
    private JedisCluster cluster;
    private Set<HostAndPort> nodes;
    private String hosts;

    public JedisClusterHelper(String hosts) {
        this.hosts = hosts;
    }
    
    public void init() {
        try {
            this.nodes = new HashSet<HostAndPort>();
            for (String host : this.hosts.split(",")) {
                String[] arr = host.split(":");
                nodes.add(new HostAndPort(arr[0], Integer.parseInt(arr[1])));
            }
            cluster = new JedisCluster(nodes);
        } catch (Exception e) {
            logger.error("init redis cluster error", e);
        }
    }
    
    public void close() {
        try {
            cluster.close();
        } catch (Exception e) {
            logger.error("close error", e);
        }   
    }
    
    public void addPoi(String redisKey, double wgs_longitude, double wgs_latitude, String uid) {
        try {
            cluster.geoadd(redisKey, wgs_longitude, wgs_latitude, uid);
        } catch (Exception e) {
            logger.error("add poi error", e);
        }  
    }
    
    public void addBatchPoi(String redisKey, List<Poi> pois) {
        try {
            for (Poi p : pois) {
                cluster.geoadd(redisKey, p.getWgs_lng(), p.getWgs_lat(), p.getUid());
            }
        } catch (Exception e) {
            logger.error("add poi error", e);
        }  
    }
    
    public void addBatchPoiInfo(List<PoiModel> pois) {
        try {
            for (PoiModel poi : pois) {
                cluster.set(poi.getUid(), JSON.toJSONString(poi));
            }
        } catch (Exception e) {
            logger.error("add poi error", e);
        }  
    }
    
    public void queryPoiRudi(String redisKey, double longitude, double latitude, double radius) {
        try {
            GeoRadiusParam param = GeoRadiusParam.geoRadiusParam();
            param.withCoord();
            param.withDist();
            List<GeoRadiusResponse> results = cluster.georadius(redisKey, longitude, latitude, radius, GeoUnit.M, param);
            logger.info("size=" + results.size());
            for (GeoRadiusResponse response : results) {
                String uid = response.getMemberByString();
                if (null != response) {
                    try {
                        GeoCoordinate coor = response.getCoordinate();
                        double distance = response.getDistance();
                        double lng = coor.getLongitude();
                        double lat = coor.getLatitude();
                        logger.info("reponse:" + uid + " " + lng + "," + lat + " " + distance);    
                    } catch (Exception e) {
                        logger.error("error", e);
                    }
                    
                }
                
            }
        } catch (Exception e) {
            logger.error("query poi error", e);
        }  
    }
    
    public static void main(String[] args) {
        JedisClusterHelper helper = new JedisClusterHelper(args[0]);
        helper.queryPoiRudi(Common.BAIDU_POI_REDIS_KEY, Double.parseDouble(args[0]), Double.parseDouble(args[1]), Double.parseDouble(args[2]));
    }
    
}
