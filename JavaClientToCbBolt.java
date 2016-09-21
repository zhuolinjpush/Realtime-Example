package cn.test.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class JavaClientToCbBolt extends BaseRichBolt {
    
    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(JavaClientToCbBolt.class);

    private static String CB_CACHE_KEY = "%st-%d";
    
    private int keyExp = 86400;
    public static Cluster cluster;
    public static Bucket bucket;
    private Map<String, MutableLong> msgQueue;
    
    private int batchMill = 2000;
    private long preMill = 0;
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        msgQueue = new HashMap<String, MutableLong>();
        int dayExp = SystemConfig.getIntProperty("msgid-stats.couchbase.exp", 5);
        keyExp = dayExp * 86400;
        initCb();
        preMill = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        if (Constants.SRC_STREAM.equals(tuple.getSourceStreamId())) {
            try {
                logger.info("target-user-bolt:" + tuple.getValues());  
                Integer active_user = tuple.getIntegerByField("active_user");                   
                if (active_user <= 0) {
                    return;
                }
                long msg_id = tuple.getLongByField("msg_id");
                String platform = tuple.getStringByField("platform");
                String key = String.format(CB_CACHE_KEY, platform, msg_id);
                incr(key, active_user);
            } catch (Exception e) {
                logger.error(String.format("bolt error: %s", tuple.getValues()), e);
            }
        }
        long currMill = System.currentTimeMillis();
        if((currMill - preMill ) > this.batchMill ) {
            if ( msgQueue.isEmpty() ) {
                logger.info("execute msgQueue is empty.");
            } else {
                commit();
            }
            preMill = currMill;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private void initCb() {
        closeCB();
        int count = 0;
        while (bucket == null) {
            count += 1;
            String nodeStr = SystemConfig.getProperty("cbnodes.couchbase.host");
            String bucketStr = SystemConfig.getProperty("msgid-stats.couchbase.bucket");
            String pwdStr = SystemConfig.getProperty("msgid-stats.couchbase.pass");
            logger.info(nodeStr + " " + bucketStr + " " + pwdStr);
            List<String> nodes = new ArrayList<String>();
            for (String node : nodeStr.split(",")) {
                nodes.add(node);
            }
            CouchbaseEnvironment environment = DefaultCouchbaseEnvironment
                                                .builder()
                                                .connectTimeout(10000)
                                                .build();
            cluster = CouchbaseCluster.create(environment, nodes);
            bucket = cluster.openBucket(bucketStr, pwdStr);

            logger.info("bucket=" + bucket);
            if (count > 5) {
                logger.error("cb client null exit");
                break;
            }
        }
        if (null == bucket ) {
            logger.info("client is null");
        } else {
            logger.info("Init couchbase ok! count is " + count);
        }
    }
    
    private void incr(String key, Integer value) {
        MutableLong newValue = new MutableLong(value);
        MutableLong oldValue = msgQueue.put(key, newValue);
        if(null != oldValue) {
            newValue.setVal(oldValue.getVal() + newValue.getVal());
        }
    }
    
    private void closeCB() {
        try {
            if (null != bucket) {
                bucket.close();
            }
            if (null != cluster) {
                cluster.disconnect();
            }           
        } catch (Exception e) {
            logger.error("close cb error", e);
        }
    }
    
    private void commit() {
        logger.info(String.format("batch commit count %d.", msgQueue.size()));
        try {
            if (null == bucket || null == cluster) {
                initCb();
            }
            Iterator<Map.Entry<String, MutableLong>> it = msgQueue.entrySet().iterator();
            Map.Entry<String, MutableLong> entry;
            String key;
            long val;
            while (it.hasNext()) {
                entry = it.next();
                key = entry.getKey();
                val = entry.getValue().getVal();
                try {
                    //JsonLongDocument doc = bucket.counter(key, val, val, keyExp, 5, TimeUnit.SECONDS);
                    JsonLongDocument doc = bucket.counter(key, val, 0, keyExp, 15, TimeUnit.SECONDS);
                    logger.info("incr " + key + " " + val + " ," + doc.cas() + " " + doc.expiry());
                    it.remove();
                } catch (Exception e) {
                    logger.error("batch commit error ", e);
                    if (null == bucket || bucket.isClosed()) {
                        initCb();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("batch commit error", e);
        }
    }
}
