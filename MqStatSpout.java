package cn.test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class MqStatSpout extends BaseRichSpout {

	private static final long serialVersionUID = -968433639730500851L;
	private static Logger LOG = LoggerFactory.getLogger(MqStatSpout.class);
	
	
	private static JsonParser jp = new JsonParser();
	private static int MQ_RECONN_SLEEP = 3000;//ms
	
	private ThRabbitLoader loader = null;
	private String key = null;
	private String[] mqlist = null;
	private SpoutOutputCollector collector = null;
	
	public StatSpout() {
		mqlist = SystemConfig.getPropertyArray("-report.list");
	}
	
	public StatSpout(String key) {
		this.key = key;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		
		int taskId = context.getThisTaskId();
		int index = taskId % mqlist.length;
		this.key = mqlist[index];
		
		this.loader = conn();
	}

	@Override
	public void nextTuple() {

		GetResponse response = null;
		
		try {
			response = loader.get();
		} catch (AlreadyClosedException ace) {
			LOG.error("mq already closed error", ace);
			this.loader = conn();
		} catch (Exception e) {
			LOG.error("mq error", e);
			this.loader = conn();
		}
		
		if( null != response ) {
			try {
				String body = new String( response.getBody(), "utf-8" );
				
				LOG.info(String.format("%s data:%s", key, body));
				
				JsonObject jObj = (JsonObject) jp.parse(body);
				
				JsonArray arr = jObj.get("rows").getAsJsonArray();
				
				for (int index = 0; index < arr.size(); index ++) {
					process( arr.get(index).getAsJsonObject() );
				}
				
			} catch (UnsupportedEncodingException e) {
				LOG.error("response format error " + response.getBody(), e);	
			} catch (Exception e) {
				LOG.error("jObj report data error " + response.getBody(), e);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Common.SRC_STREAM,
				new Fields("msg_id", "uid", "appkey", "itime", "status"));
		
		declarer.declareStream(Common.SHUTDOWN_STREAM,
				new Fields(Common.SHUTDOWN_STREAM));
	}

	@Override
	public void close() {

	}

	@Override
	public void deactivate() {
		LOG.info("spout deactivate");
		try {
			releaseMQ();
		} catch (Exception e) {
			LOG.error("spout deactivate error !", e);
		}
		this.collector.emit(Common.SHUTDOWN_STREAM,
				new Values(Common.SHUTDOWN_STREAM));
	}
	
	private void releaseMQ() {
		if (null != loader) {
			loader.close();
			loader = null;
		}
	}
	
	private ThRabbitLoader conn() {
		
		ThRabbitLoader loader = null;
		int i = 0;
		while (i < 5) {
			try {
				loader = new ThRabbitLoader(this.key, false, new HashMap<String, Object>());
				LOG.info("connect mq success " + this.key);
				break;
			} catch (Exception e) {
				i++;
				LOG.error("conn mq error", e);
				try {
					Thread.sleep(MQ_RECONN_SLEEP);
				} catch (InterruptedException e1) {
					LOG.error("mq reconn sleep error", e);
				}
			}
		}
		return loader;
	}
	
	private void process(JsonObject obj) {
		try {
            ///////////example
			long itime = obj.get("itime").getAsLong();
			String status = obj.get("status").getAsString();
			collector.emit(Common.SRC_STREAM,
					new Values(msg_id, uid, appkey, itime, status));
		} catch (Exception e) {
			LOG.error("process data:" + obj, e);
		}
	}
	
}
