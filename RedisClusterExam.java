package cn.jpush.helper;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisClusterHelper {

    private static Logger logger = LoggerFactory.getLogger(JedisClusterHelper.class);
    private JedisCluster cluster;
    private Set<HostAndPort> nodes;
    private String hosts;

    public JedisClusterHelper(String hosts) {
        this.hosts = hosts;
        init();
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
    
    public void pfadd(String key, String... list) {
        try {
            cluster.pfadd(key, list);
        } catch (JedisConnectionException jce) {
            logger.error("redis connection error", jce);
            init();
        } catch (Exception e) {
            logger.error("pfadd error," + key, e);
        } 
        
    }
}

