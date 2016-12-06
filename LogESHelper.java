package cn.helper;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.common.Constants;
import cn.crash.CrashLogVo;
import cn.utils.SystemConfig;

public class CrashLogESHelper {

    private static Logger logger = LoggerFactory.getLogger(CrashLogESHelper.class);
    
    private Settings settings;
    private TransportClient client = null;
    private List<InetSocketTransportAddress> ISTAList;
    private static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static MessageDigest md;
    
    public CrashLogESHelper() {
        initClient();
    }

    private void initClient() {
        this.settings = Settings.builder()
                .put("client.transport.sniff", SystemConfig.getBooleanProperty("crashlog.client.transport.sniff"))
                .put("client.transport.ping_timeout", SystemConfig.getProperty("crashlog.client.transport.ping_timeout"))
                .put("client.transport.nodes_sampler_interval", SystemConfig.getProperty("crashlog.client.transport.nodes_sampler_interval"))
                .put("cluster.name", SystemConfig.getProperty("crashlog.cluster.name")).build();
        
        this.ISTAList = new ArrayList<InetSocketTransportAddress>();
        String ipList = SystemConfig.getProperty("crashlog.transport.ip");
        String[] ips = ipList.split(";");
        for (int i = 0; i < ips.length; i++) {
            String addr[] = ips[i].split(":");
            try {
                ISTAList.add(new InetSocketTransportAddress(InetAddress.getByName(addr[0]), Integer.valueOf(addr[1])));
            } catch (Exception e) {
                logger.error("add transport address error", e);
            }
        }
        try {
            this.client = new PreBuiltTransportClient(this.settings);
            for (int i = 0; i < ISTAList.size(); i++) {
                logger.info(ISTAList.get(i) + "");
                client.addTransportAddress(ISTAList.get(i));
            }
        } catch (Exception e) {
            logger.error("init transport client error", e);
        }
        logger.info("create es client");;
    }

    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            logger.error("cleanup error", e);
        }
    }
    
    public static String getTime(long itime) {
        return sdf.format(new Date(itime));
    }
    
    public void process(Map<String, CrashLogVo> map) {
        logger.info("save info...");
        saveInfo(map);
        logger.info("save model...");
        saveModel(map);
        logger.info("save os...");
        saveOS(map);
    }

    public void saveInfo(Map<String, CrashLogVo> map) {
        try {
            if (client == null) {
                initClient();
            }
            List<CrashLogVo> addList = new ArrayList<CrashLogVo>();
            List<CrashLogVo> updateList = new ArrayList<CrashLogVo>();
            Map<String, Long> originErrCount = new HashMap<String, Long>();
            Set<String> existIdsSet = new HashSet<String>();
            
            //logger.info("mdcode=" + Join.join(",", map.keySet()));
            MultiGetResponse getRes = client.prepareMultiGet().add(Constants.CRASH_LOG_INFO, Constants.TYPE_V1, map.keySet()).execute().actionGet();
            if (getRes != null && getRes.getResponses() != null && getRes.getResponses().length > 0) {
                for (MultiGetItemResponse res : getRes.getResponses()) {
                    //logger.info("exist id=" + res.getId() + " " + res.getResponse().isExists());
                    if (res.getResponse().isExists() && map.containsKey(res.getId())) {
                        existIdsSet.add(res.getId());
                        CrashLogVo vo = map.get(res.getId());
                        //logger.info(res.getResponse().getSourceAsString());
                        logger.info("exist id=" + res.getId() + " happen_amount=" + res.getResponse().getSource().get("happen_amount"));
                        Object obj = res.getResponse().getSource().get("happen_amount");
                        long errCount = map.get(res.getId()).getErrcount();
                        //logger.info("errCount=" + errCount);
                        //record origin errcount
                        originErrCount.put(res.getId(), errCount);
                        vo.setErrcount(Long.parseLong(String.valueOf(obj)) + errCount);
                        updateList.add(vo);
                    }
                }
            }

            for (String key : map.keySet()) {
                if (!existIdsSet.contains(key)) {//add
                    addList.add(map.get(key));
                }
            }
            //add
            if (addList.size() > 0) {
                logger.info("addlist size=" + addList.size());
                addCrashLogInfo(addList);
            }
            //update
            if (updateList.size() > 0) {
                logger.info("updateList size=" + updateList.size());
                updateCrashLogInfo(updateList);
            }
            
            //origin errcount
            logger.info("origin err size=" + originErrCount.size());
            for (String k : originErrCount.keySet()) {
                map.get(k).setErrcount(originErrCount.get(k));
            }
            
        } catch (Exception e) {
            logger.error("save error", e);
        }
    }
    
    public void saveModel(Map<String, CrashLogVo> map) {
        try {
            if (client == null) {
                initClient();
            }
            Map<String, CrashLogVo> idMap = new HashMap<String, CrashLogVo>();
            for (String mdk : map.keySet()) {
                CrashLogVo v = map.get(mdk);
                //key = mdcode + model
                idMap.put(getMD5(mdk+v.getModel()), v);
            }
            
            Map<String, Long> updateMap = new HashMap<String, Long>();
            MultiGetResponse getRes = client.prepareMultiGet().add(Constants.CRASH_LOG_MODEL, Constants.MODE_V1, idMap.keySet()).execute().actionGet();           
            if (getRes != null && getRes.getResponses() != null && getRes.getResponses().length > 0) {
                for (MultiGetItemResponse res : getRes.getResponses()) {
                    //logger.info("exist id=" + res.getId() + " " + res.getResponse().isExists());
                    if (res.getResponse().isExists()) {
                        long amount = Long.parseLong(String.valueOf(res.getResponse().getSource().get("amount")));
                        logger.info("id=" + res.getId() + ", model amount=" + amount);
                        updateMap.put(res.getId(), amount + idMap.get(res.getId()).getErrcount());
                    }
                    
                }
            }
            
            //update
            if (updateMap.size() > 0) {
                updateCrashLogModel(updateMap);               
            }
            Map<String, CrashLogVo> addMap = new HashMap<String, CrashLogVo>();
            for (String idK : idMap.keySet()) {
                if (!updateMap.keySet().contains(idK)) {
                    addMap.put(idK, idMap.get(idK));
                } 
            }
            //add
            if (addMap.size() > 0) {
                addCrashLogModel(addMap);
            }

        } catch (Exception e) {
            logger.error("save error", e);
        }
    }
    
    public void saveOS(Map<String, CrashLogVo> map) {
        try {
            if (client == null) {
                initClient();
            }
            
            Map<String, CrashLogVo> idMap = new HashMap<String, CrashLogVo>();
            for (String mdk : map.keySet()) {
                CrashLogVo v = map.get(mdk);
                //key = mdcode + os
                idMap.put(getMD5(mdk+v.getOs_version()), v);
            }
            
            Map<String, Long> updateMap = new HashMap<String, Long>();
            MultiGetResponse getRes = client.prepareMultiGet().add(Constants.CRASH_LOG_OS, Constants.OS_V1, idMap.keySet()).execute().actionGet();           
            if (getRes != null && getRes.getResponses() != null && getRes.getResponses().length > 0) {
                for (MultiGetItemResponse res : getRes.getResponses()) {
                    //logger.info("exist id=" + res.getId() + " " + res.getResponse().isExists());
                    if (res.getResponse().isExists()) {
                        long amount = Long.parseLong(String.valueOf(res.getResponse().getSource().get("amount")));
                        logger.info("id=" + res.getId() + ", os amount=" + amount);
                        updateMap.put(res.getId(), amount + idMap.get(res.getId()).getErrcount());
                    }
                }
            }

            //update
            if (updateMap.size() > 0) {
                updateCrashLogOS(updateMap);               
            }
            
            Map<String, CrashLogVo> addMap = new HashMap<String, CrashLogVo>();
            for (String idK : idMap.keySet()) {
                if (!updateMap.keySet().contains(idK)) {
                    addMap.put(idK, idMap.get(idK));
                } 
            }
            
            //add
            if (addMap.size() > 0) {
                addCrashLogOS(addMap);
            }
        } catch (Exception e) {
            logger.error("save error", e);
        }
    }
    
    public void addBulk(Map<String, String> addMap, boolean autoGeneId, String index, String type) {
        try {
            BulkRequestBuilder builder = client.prepareBulk();
            for (String key : addMap.keySet()) {
                if (autoGeneId) {
                    builder.add(client.prepareIndex(index, type).setSource(addMap.get(key)));
                } else {
                    builder.add(client.prepareIndex(index, type, key).setSource(addMap.get(key)));
                }
                 
            }
            BulkResponse resp = builder.get();
            logger.info("Bulk " + index + " " + type + " size= " + addMap.size() + " done cost= "+ resp.getTookInMillis()+ " ms");
            if (resp.hasFailures()) {
                logger.error("Bulk has failures.");
                for ( BulkItemResponse bir : resp.getItems()) {
                    if (bir.isFailed()) {
                        logger.error("Error indexing item:{}", bir.getItemId() );
                        logger.error("Error message:{}", bir.getFailureMessage());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("add error", e);
        }
    }
    
    public void updateBulk(Map<String, String> updateMap, String index, String type) {
        try {
            BulkRequestBuilder builder = client.prepareBulk();
            for (String key : updateMap.keySet()) {
                builder.add(client.prepareUpdate(index, type, key).setDoc(updateMap.get(key)));
            }
            BulkResponse resp = builder.get();
            logger.info("Bulk " + index + " " + type + " size= " + updateMap.size() + " done cost= "+ resp.getTookInMillis()+ " ms");
            if (resp.hasFailures()) {
                logger.error("Bulk has failures.");
                for ( BulkItemResponse bir : resp.getItems()) {
                    if (bir.isFailed()) {
                        logger.error("Error indexing item:{}", bir.getItemId() );
                        logger.error("Error message:{}", bir.getFailureMessage());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("update error", e);
        }
    }
    
    public void addCrashLogInfo(List<CrashLogVo> addList) {
        try {
            Map<String, String> addMap = new HashMap<String, String>();
            for (CrashLogVo vo : addList) {
                String json = XContentFactory.jsonBuilder().startObject()
                                             .field("app_key", vo.getApp_key())
                                             .field("platform", vo.getPlatform())
                                             .field("versionname", vo.getVersionname())
                                             .field("happen_amount", vo.getErrcount())
                                             .field("affectuser", 1L)
                                             .field("starttime", getTime(vo.getCrashtime()))
                                             .field("endtime", getTime(vo.getCrashtime()))
                                             .field("status", 1)
                                             .field("message", vo.getMessage())
                                             .field("stacktrace", vo.getStacktrace())
                                             .field("info", vo.getInfo())
                                             .field("update_time", getTime(System.currentTimeMillis()))
                                             .endObject().string();
                addMap.put(vo.getMdcode(), json);
                //logger.info(json);
            }
            addBulk(addMap, false, Constants.CRASH_LOG_INFO, Constants.TYPE_V1);
        } catch (Exception e) {
            logger.error("add error", e);
        }
    }
    
    public void updateCrashLogInfo(List<CrashLogVo> updateList) {
        try {
            Map<String, String> updateMap = new HashMap<String, String>();
            for (CrashLogVo vo : updateList) {
                String json = XContentFactory.jsonBuilder().startObject()
                                             .field("endtime", getTime(vo.getCrashtime()))
                                             .field("happen_amount", vo.getErrcount())
                                             .field("update_time", getTime(System.currentTimeMillis()))
                                             .endObject()
                                             .string();
                updateMap.put(vo.getMdcode(), json);
                //logger.info(json);
            }
            updateBulk(updateMap, Constants.CRASH_LOG_INFO, Constants.TYPE_V1);
        } catch (Exception e) {
            logger.error("update error", e);
        }
    }
    
    public void addCrashLogModel(Map<String, CrashLogVo> map) {
        try {
            Map<String, String> addMap = new HashMap<String, String>();
            for (String id : map.keySet()) {
                CrashLogVo vo = map.get(id);
                String json = XContentFactory.jsonBuilder().startObject()
                        .field("mdcode", vo.getMdcode())
                        .field("app_key", vo.getApp_key())
                        .field("model", vo.getModel())
                        .field("amount", vo.getErrcount())
                        .field("update_time", getTime(System.currentTimeMillis()))
                        .endObject().string();
                addMap.put(id, json);
                //logger.info(json);
            }
            addBulk(addMap, false, Constants.CRASH_LOG_MODEL, Constants.MODE_V1);
        } catch (Exception e) {
            logger.error("add error", e);
        }
    }
    
    public void updateCrashLogModel(Map<String, Long> map) {
        try {
            Map<String, String> updateMap = new HashMap<String, String>();
            for (String id : map.keySet()) {
                String json = XContentFactory.jsonBuilder().startObject()
                        .field("amount", map.get(id))
                        .field("update_time", getTime(System.currentTimeMillis()))
                        .endObject().string();
                updateMap.put(id, json);
                //logger.info(json);
            }
            updateBulk(updateMap, Constants.CRASH_LOG_MODEL, Constants.MODE_V1);
        } catch (Exception e) {
            logger.error("update error", e);
        }
    }
    
    public void addCrashLogOS(Map<String, CrashLogVo> map) {
        try {
            Map<String, String> addMap = new HashMap<String, String>();
            for (String id : map.keySet()) {
                CrashLogVo vo = map.get(id);
                String json = XContentFactory.jsonBuilder().startObject()
                        .field("mdcode", vo.getMdcode())
                        .field("app_key", vo.getApp_key())
                        .field("os_version", vo.getOs_version())
                        .field("amount", vo.getErrcount())
                        .field("update_time", getTime(System.currentTimeMillis()))
                        .endObject().string();
                addMap.put(id, json);
                //logger.info(json);
            }
            addBulk(addMap, false, Constants.CRASH_LOG_OS, Constants.OS_V1);
        } catch (Exception e) {
            logger.error("add error", e);
        }
    }
    
    public void updateCrashLogOS(Map<String, Long> map) {
        try {
            Map<String, String> updateMap = new HashMap<String, String>();
            for (String id : map.keySet()) {
                String json = XContentFactory.jsonBuilder().startObject()
                        .field("amount", map.get(id))
                        .field("update_time", getTime(System.currentTimeMillis()))
                        .endObject().string();
                updateMap.put(id, json);
                //logger.info(json);
            }
            updateBulk(updateMap, Constants.CRASH_LOG_OS, Constants.OS_V1);
        } catch (Exception e) {
            logger.error("update error", e);
        }
    }
    
    public String getMD5(String key) {
        String result = null;
        try {
            if (null == md) {
                md = MessageDigest.getInstance("MD5");
            }
            md.update(key.getBytes());
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            result = buf.toString();
        } catch (Exception e) {
            logger.error("get md5 key error", e);
        }
        return result;
    } 

    public static void main(String[] args) {

    }

}
