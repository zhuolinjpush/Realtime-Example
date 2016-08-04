package cn.cb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import cn.test.Constants;
import cn.test.utils.SystemConfig;

public class MsgSendTimeTopo {

    private static Logger logger = LoggerFactory.getLogger(MsgSendTimeTopo.class);
    
    public static void main(String[] args) {
        String topoName = "msg-sendtime-cb";
        int spoutNum = 1;
        int boltNum = 1;
        int workNum = 1;
        if (args.length == 4) {
            spoutNum = Integer.parseInt(args[0]);
            boltNum = Integer.parseInt(args[1]);
            workNum = Integer.parseInt(args[2]);
            topoName = args[3];
        }
        logger.info(String.format("topo[%s], spout[%d], bolt[%d], work[%d]", topoName, spoutNum, boltNum, workNum));
        
        String topicNV2 = "_v2";
        String topicNV3 = "_v3";
        String topicVV2 = "_v2_vip";
        String topicVV3 = "_v3_vip";
        String groupId = "send-time";

        String spoutNv2 = "normalV2";
        String spoutNv3 = "normalV3";
        String spoutVv2 = "VipV2";
        String spoutVv3 = "VipV3";
        String boltPortal = "pBolt";
        String toCbBolt = "Sendtime2CB";
        
        String zookeeper = SystemConfig.getProperty("kafka.zookeeper.connect");
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(spoutNv2, new MsgPushApiSpout(zookeeper, topicNV2, groupId), spoutNum);
        builder.setSpout(spoutNv3, new MsgPushApiSpout(zookeeper, topicNV3, groupId), spoutNum);
        builder.setSpout(spoutVv2, new MsgPushApiSpout(zookeeper, topicVV2, groupId), spoutNum);
        builder.setSpout(spoutVv3, new MsgPushApiSpout(zookeeper, topicVV3, groupId), spoutNum * 2);
        
        builder.setBolt(boltPortal, new MsgPortalBolt(), boltNum)
                .shuffleGrouping(spoutNv2, Constants.PORTAL_STREAM)
                .shuffleGrouping(spoutNv3, Constants.PORTAL_STREAM)
                .shuffleGrouping(spoutVv2, Constants.PORTAL_STREAM)
                .shuffleGrouping(spoutVv3, Constants.PORTAL_STREAM);
        
        builder.setBolt(toCbBolt, new SaveToCBBolt(), boltNum * 4)
                .shuffleGrouping(spoutNv2, Constants.TOCB_STREAM)
                .shuffleGrouping(spoutNv3, Constants.TOCB_STREAM)
                .shuffleGrouping(spoutVv2, Constants.TOCB_STREAM)
                .shuffleGrouping(spoutVv3, Constants.TOCB_STREAM)
                .shuffleGrouping(boltPortal, Constants.TOCB_STREAM);
                        
        Config config = new Config();
        config.setNumAckers(0);
        config.setNumWorkers(workNum);
        config.setMaxSpoutPending(5000);
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        
        try {
            StormSubmitter.submitTopology(topoName,
                    config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            logger.error("job exists", e);
        } catch (InvalidTopologyException e) {
            logger.error("job invalid", e);
        } catch (Exception e) {
            logger.error("job assign error", e);
        }  

    }

}
