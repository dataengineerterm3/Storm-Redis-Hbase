package storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by CLY on 2015/5/28.
 */
public class DataBolt extends BaseBasicBolt{
    public Configuration config;
    public HTable table;
    public void prepare(Map stormConf, TopologyContext context) {
        config = HBaseConfiguration.create();
        try {
            table = new HTable(config, Bytes.toBytes("storm-hbase2"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String [] arr = input.getString(0).split(",",-1);
        String rowkey = arr[0];
        String category = arr[1];
		String rating = arr[2];
		String img = arr[3];
		String address =arr[4];
		String phone = arr[5];
		String comment = arr[6];
		
        Put put = new Put(Bytes.toBytes(rowkey));

        try {
           
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("category"),
                    Bytes.toBytes(category));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("rating"),
                    Bytes.toBytes(rating));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("img"),
                    Bytes.toBytes(img));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("address"),
                    Bytes.toBytes(address));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("phone"),
                    Bytes.toBytes(phone));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("comment"),
                    Bytes.toBytes(comment));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanup() {
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("title", 
				"categories",
				"rating",
				"img", 
				"address", 
				"phone", 
				"comment"));
    }

}