package cn.stats.fix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PushMqFix {

    private static Logger LOG = LoggerFactory.getLogger(PushMqFix.class);

    private Channel  mqChannel = null;
    private Connection mqConnection = null;
    private String host;
    private int port;
    private String user;
    private String password;
    private String exchange;
    private String routingKey;    

    public PushMqFix(String host, int port, String user, String password,
            String exchange, String routingKey) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    private void init() {
        close();
        try {
            Address[] mq_addrs = Address.parseAddresses(this.host + ":" + this.port);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(this.user);
            factory.setPassword(this.password);
            mqConnection = factory.newConnection(mq_addrs);
            mqChannel = mqConnection.createChannel();
            LOG.info("create mq channel ok!");
        } catch (Exception e) {
            LOG.error("init backup mq error", e);
        }
    }
    
    private void close() {
        try {
            if (null != mqChannel) {
                mqChannel.close();
            }
            if (null != mqConnection) {
                mqConnection.close();
            }
        } catch (Exception e) {
            LOG.error("close mq error", e);
        }
    }

    public void process(String filename) {
        File file = new File(filename);
        if (!file.exists() || !file.isFile()) {
            LOG.warn("file is invalid, " + filename);
            return;
        }
        init();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            long count = 0;
            while ((line = reader.readLine()) != null) {
                count++;
                try {
                    mqChannel.basicPublish(this.exchange, this.routingKey, null, line.trim().getBytes());
                } catch (Exception e) {
                    LOG.error("publish error", e);
                    init();
                }
                if (count % 1000 == 0) {
                    LOG.info("count=" + count);
                }
            }
            reader.close();            
        } catch (Exception e) {
            LOG.error("process error", e);
        } finally {
            close();
        }
    }

    public static void main(String[] args) {
        PushMqFix obj = new PushMqFix(args[0], Integer.parseInt(args[1]), args[2], args[3], args[4], args[5]);
        obj.process(args[6]);
    }

}
