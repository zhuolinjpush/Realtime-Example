package cn.test.spout;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import net.spy.memcached.OperationTimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ApnsKafkaToCBSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(ApnsKafkaToCBSpout.class);
    private final String m_zookeeper;
    private final String m_topic;
    private final String m_groupId;
    private SpoutOutputCollector collector;
    private boolean deactivate = false;
    private KafkaStream<byte[], byte[]> m_stream;
    private ConsumerConnector consumer;
    private static JsonParser jp = new JsonParser();
    private int mills;
    
    //msg
    private static final String MSG_CB_STAT_KEY = "realtime-stat";
    private static final int MSG_CB_EXP = 3600 * 24 * 5;
    private CouchbaseClient msgCBCli = null;
    private Map<String, MutableLong> msgQueue = new HashMap<String, MutableLong>();
    private long msgMill = 0;

    public ApnsKafkaToCBSpout(String m_zookeeper, String m_topic, String m_groupId, int sec) {
        this.mills = sec * 1000;
        this.m_zookeeper = m_zookeeper;
        this.m_topic = m_topic;
        this.m_groupId = m_groupId;
        this.msgMill = System.currentTimeMillis();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        this.collector = collector;
        openStream();
        initMsgCB();
    }

    @Override
    public void nextTuple() {
        logger.info("nextTuple");
        if(null == m_stream) {
            openStream();
        }
        try{
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()) {
                byte[] ser = it.next().message();
                if(null != ser) {
                    String body = new String(ser);
                    logger.info("kafka apns-data:" + body);
                    try {
                        JsonObject apns = (JsonObject) jp.parse(body);
                        JsonArray arr = apns.get("rows").getAsJsonArray();
                        for (int index = 0; index < arr.size(); index ++) {
                            process( arr.get(index).getAsJsonObject() );
                        }  
                    } catch (Exception e) {
                        logger.error("Failed to process " + body, e);
                    }
                } else {
                    try{
                        Thread.sleep(200);
                    } catch (Exception e) {
                        logger.error("Sleep interrupted.", e);
                    }
                }
                
            } // while it.hasNext()
        } catch (Exception e) {
            logger.error("Failed to consume", e);
            if( !deactivate ) {
                openStream();
            }
        }
        if (!msgQueue.isEmpty()) {
            logger.info("next tuple end");
            commit();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Common.SRC_STREAM,
                new Fields("msg_id", "uid", "appkey", "itime", "status"));
        
        declarer.declareStream(Common.SHUTDOWN_STREAM,
                new Fields(Common.SHUTDOWN_STREAM));      
    }
    
    private void process(JsonObject obj) {
        try {
            long msg_id = obj.get("msg_id").getAsLong();
            long uid = obj.get("uid").getAsLong();
            String appkey = obj.get("appkey").getAsString();
            long itime = obj.get("itime").getAsLong();
            String status = obj.get("status").getAsString();
            collector.emit(Common.SRC_STREAM, new Values(msg_id, uid, appkey, itime, status));
            //msg
            handleMsg(msg_id, status);         
            
        } catch (Exception e) {
            logger.error("process data:" + obj, e);
        }
    }
    
    public void handleMsg(long msg_id, String status) {
        try {
            //logger.info("msg incr:" + msg_id + " " + status);
            switch(status) {
            case "failed":
                incrMsg("msg-failed-i-" + msg_id);
                break;
            case "success":
                incrMsg("msg-total-i-" + msg_id);
                break;
            case "response":
                incrMsg("msg-response-i-" + msg_id);
                break;
            default:
                logger.warn("the status is incorrect " + msg_id + " " + status);
            }
            long currMill = System.currentTimeMillis();
            if((currMill - msgMill) > this.mills) {
                if( msgQueue.isEmpty() ) {
                    logger.info("execute queue is empty.");
                } else {
                    //logger.info("msgqueue size=" + msgQueue.size());
                    commit();
                }
                msgMill = currMill;
            }
        } catch (Exception e) {
            logger.error("apns msg count error " + msg_id, e);
        }
    }

    private void incrMsg(String key) {
        MutableLong newValue = new MutableLong(1L);
        MutableLong oldValue = msgQueue.put(key, newValue);
        if(null != oldValue) {
            newValue.setVal(oldValue.getVal() + 1);
        }
    }
    
    private void commit() {
        logger.info("commit msgQueue size " + msgQueue.size());
        if ( null == msgCBCli ) {
            initMsgCB();
        }
        Iterator<Entry<String, MutableLong>> it = msgQueue.entrySet().iterator();
        Entry<String, MutableLong> entry;
        String key;
        while(it.hasNext()) {
            entry = it.next();
            key = entry.getKey();
            long val = entry.getValue().getVal();
            try {
                msgCBCli.incr(key, val, val, MSG_CB_EXP);
                logger.info("apnsincr " + key + " " + val);
                it.remove();
            } catch (OperationTimeoutException te) {
                logger.error("incr timeout error.msg-queue-size={} {} {}", msgQueue.size(), key, val, te);
                CouchBaseClientFactory.shutdown(msgCBCli);
                msgCBCli = null;
                initMsgCB();
            } catch (Exception e) {
                logger.error("count commit error.{} {}", key, val, e);
            }
        }
    }
    
    @Override
    public void close() {
        deactivate();
    }

    @Override
    public void deactivate() {
        if (!msgQueue.isEmpty()) {
            commit();
        }
        logger.info("deactivate");
        deactivate = true;
        shutdownConsumer();
        this.collector.emit(Common.SHUTDOWN_STREAM, new Values(Common.SHUTDOWN_STREAM));
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("offsets.storage", "kafka");
        props.put("dual.commit.enabled", "false");
        props.put("auto.commit.interval.ms", "2000");
        props.put("zookeeper.session.timeout.ms", "12000");
        props.put("zookeeper.connection.timeout.ms", "10000");
        props.put("zookeeper.connect", m_zookeeper);
        props.put("group.id", m_groupId);
        return new ConsumerConfig(props);
    }

    private void openStream() {
        try{
            shutdownConsumer();
            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(m_topic, new Integer(1));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_topic);
            logger.info("stream size is " + streams.size());
            m_stream = streams.get(0);
        } catch (Exception e) {
            logger.error("Failed to open stream", e);
        }
    }

    private void shutdownConsumer() {
        if(null != consumer) {
            try {
                consumer.shutdown();
            } catch (Exception e) {
                logger.error("Failed to shutdown consumer", e);
            }
        }
    }
    
    /**
     * msg cb sum
     */
    private void initMsgCB() {
        int ccount = 0;
        while(null == msgCBCli ) {
            logger.error("client is null");
            ccount += 1;
            msgCBCli = CouchBaseClientFactory.initCouchbaseClient(MSG_CB_STAT_KEY);
            if (ccount > 5) {
                logger.error("client null exit");
                break;
            }
        }
        if (null == msgCBCli ) {
            logger.info("client is null");
        } else {
            logger.info("Init couchbase ok! count is " + ccount);
        }
    }

}
