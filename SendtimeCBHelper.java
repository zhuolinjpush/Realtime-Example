package cn.msg.helper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.utils.SystemConfig;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class SendtimeCBHelper {

    private static Logger logger = LoggerFactory.getLogger(SendtimeCBHelper.class);
    private  CouchbaseEnvironment env;
    private Cluster cluster;
    private Bucket bucket;
    
    public SendtimeCBHelper() {
        init();
    }
    
    public void init() {
        try {
            
            String[] nodes = SystemConfig.getPropertyArray("ApiSendtime.couchbase.node");
            String bucket = SystemConfig.getProperty("ApiSendtime.couchbase.bucket");
            String password = SystemConfig.getProperty("ApiSendtime.couchbase.pass");
            logger.info("init: " + nodes + " " + bucket + " " + password);
            env = DefaultCouchbaseEnvironment.builder()
                    .connectTimeout(TimeUnit.SECONDS.toMillis(30))
                    .build();
            this.cluster = CouchbaseCluster.create(env, nodes);
            this.bucket = this.cluster.openBucket(bucket, password);
            
            logger.info("init success!");
        } catch (Exception e) {
            logger.error("init cb error", e);
        }
    }
    
    public void close() {
        try {
            this.cluster.disconnect();
        } catch (Exception e) {
            logger.error("close error", e);
        }
    }
    
    public JsonDocument get(String key) {
        return this.bucket.get(key);
    }
    
    public StringDocument getSendtime(String key) {        
        return this.bucket.get(key, CouchbaseAsyncBucket.STRING_TRANSCODER.documentType());
    }
    
    public Map<String, Object> query(Set<String> rowkeys) {
        long start = System.currentTimeMillis();
        Map<String, Object> data = new HashMap<String, Object>();
        try {
            for (String key : rowkeys) {
                StringDocument json = getSendtime(key);
                logger.info(key + " " + json + " " + json.content());
                if (null == json || json.content().isEmpty()) {
                    continue;
                }
                logger.info(key + " " + json.content().toString());
                data.put(key, json.content().toString());
            }
        } catch (Exception e) {
            logger.error("query error", e);
        }
        logger.info("query cb size=" + rowkeys.size() + " ,result=" + data.size() + " ,cost=" + (System.currentTimeMillis() - start));
        return data;        
    }

    
    public static void main(String[] args) {
        Set<String> list = new HashSet<String>();
        for (String s : args[0].split(",")) {
            list.add(s);
        }
        SendtimeCBHelper helper = new SendtimeCBHelper();
        Map<String, Object> data = helper.query(list);
        for (String key : data.keySet()) {
            logger.info(key + " " + Long.parseLong(String.valueOf(data.get(key))));
        }

    }

}

-----------------------------
        transcoders = new ConcurrentHashMap<Class<? extends Document>, Transcoder<? extends Document, ?>>();
        transcoders.put(JSON_OBJECT_TRANSCODER.documentType(), JSON_OBJECT_TRANSCODER);
        transcoders.put(JSON_ARRAY_TRANSCODER.documentType(), JSON_ARRAY_TRANSCODER);
        transcoders.put(JSON_BOOLEAN_TRANSCODER.documentType(), JSON_BOOLEAN_TRANSCODER);
        transcoders.put(JSON_DOUBLE_TRANSCODER.documentType(), JSON_DOUBLE_TRANSCODER);
        transcoders.put(JSON_LONG_TRANSCODER.documentType(), JSON_LONG_TRANSCODER);
        transcoders.put(JSON_STRING_TRANSCODER.documentType(), JSON_STRING_TRANSCODER);
        transcoders.put(RAW_JSON_TRANSCODER.documentType(), RAW_JSON_TRANSCODER);
        transcoders.put(LEGACY_TRANSCODER.documentType(), LEGACY_TRANSCODER);
        transcoders.put(BINARY_TRANSCODER.documentType(), BINARY_TRANSCODER);
        transcoders.put(STRING_TRANSCODER.documentType(), STRING_TRANSCODER);
        transcoders.put(SERIALIZABLE_TRANSCODER.documentType(), SERIALIZABLE_TRANSCODER);
------------------------------

