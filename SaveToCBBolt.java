package cn.test.cb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.CouchbaseClient;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SaveToCBBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(SaveToCBBolt.class);
    private static final String COUCHBASE_KEY = "msg-sendtime";
    private int keyExp = 86400 * 7;
    private CouchbaseClient client;
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        int dayExp = SystemConfig.getIntProperty(COUCHBASE_KEY + ".couchbase.exp", 7);
        keyExp = dayExp * 86400;
        initCb();      
    }

    @Override
    public void execute(Tuple input) {
        try {
            if (input.getSourceStreamId().equals(Constants.TOCB_STREAM)) {
                //"msgid", "appkey", "itime"
                long msgid = input.getLongByField("msgid");
                String appkey = input.getStringByField("appkey").trim();
                long itime = input.getLongByField("itime");
                logger.info("to cb:" + msgid + appkey + " " + itime );
                try {
                    //itime string 
                    client.set(msgid + appkey, keyExp, String.valueOf(itime));
                } catch (Exception e) {
                    logger.error("cb set error," + msgid + " " + appkey + " " + itime, e);
                    CouchBaseClientFactory.shutdown(client);
                    initCb();
                }    
            }
        } catch (Exception e) {
            logger.error("save to cb error", e);
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    
    private void initCb() {
        int count = 0;
        while (client == null) {
            count += 1;
            this.client = CouchBaseClientFactory.initCouchbaseClient(COUCHBASE_KEY);
            if (count > 5) {
                logger.error("cb client null exit");
                break;
            }
        }
        if (null == client ) {
            logger.info("client is null");
        } else {
            logger.info("Init couchbase ok! count is " + count);
        }
    }

}
