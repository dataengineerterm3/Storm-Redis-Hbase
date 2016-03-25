package storm;

import storm.spout.RedisPubSubSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class RedisPubSubTopology {


	public static void main(String[] args) {
        args = new String[3];
        args[0] = "127.0.0.1";
        args[1] = "6379";
        args[2] = "data";
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String pattern = args[2];
        TopologyBuilder builder = new TopologyBuilder();
       
      
        builder.setSpout("pubsub", new RedisPubSubSpout(host,port,pattern));
        builder.setBolt("bolt", new DataBolt()).shuffleGrouping("pubsub");
        
        Config conf = new Config();
        conf.setDebug(true);

        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        //cluster.killTopology("test");
        //cluster.shutdown();
        //
    }
}
