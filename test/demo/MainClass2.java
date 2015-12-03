package demo;

import java.util.List;

import util.Config;

import com.aliyun.drc.client.message.DataMessage.Record;
import com.aliyun.drc.client.message.DataMessage.Record.Field;
import com.aliyun.drc.client.message.DataMessage.Record.Type;
import com.aliyun.drc.client.message.transfer.JavaObject;
import com.aliyun.drc.client.message.transfer.String2JavaObject;
import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;

public class MainClass2 {
	public static void main(String[] args) throws Exception {
		// logger.info("start");
		// 创建一个context
		RegionContext context = new RegionContext();
		// 运行SDK的服务器是否使用公网IP连接DTS
		context.setUsePublicIp(true);
		// 用户accessKey secret
		context.setAccessKey(Config.accessKey);
		context.setSecret(Config.secret);
		// 创建消费者
		final ClusterClient client = new DefaultClusterClient(context);
		// 创建订阅监听者listener
		ClusterListener listener = new ClusterListener() {

			// 这个函数主要用于监听订阅到的增量数据，然后对订阅到的数据进行消费
			@Override
			public void notify(List<ClusterMessage> messages) throws Exception {
				for (ClusterMessage message : messages) {
					// 打印订阅到的数据
					Record record = message.getRecord();
					if (record.getOpt() != Type.HEARTBEAT) {
						System.out.println("begin-----------------------------------------");
						System.out.println("dbName: " + record.getDbname());
						System.out.println("tableName: "+ record.getTablename());
						System.out.println("opt: " + record.getOpt());
						System.out.println("Checkpoint: "+ record.getCheckpoint());
						System.out.println("timestamp: "+ record.getTimestamp());
						System.out.println("primaryKeys: "+ record.getPrimaryKeys());

						if (record.getOpt() != Type.BEGIN && record.getOpt() != Type.COMMIT) {
							System.out.println("-------------------------");
							System.out.println("fieldCount: "+ record.getFieldCount());
							List<Field> fields = record.getFieldList();
							for (Field f : fields) {
								System.out.print(" Encoding: "+ f.getEncoding());
								System.out.print(" ,Fieldname: "	+ f.getFieldname());
								System.out.print(" ,Type: " + f.getType());
								JavaObject obj = String2JavaObject.field2Java(f);
								System.out.print(" ,value: " + obj.getObjectValue());
								System.out	.print(" ,isPrimary: " + f.isPrimary());
								System.out.println(" ,isChangeValue: "+ f.isChangeValue());
							}
							System.out.println("-------------------------");
						}
						System.out.println("end-----------------------------------------");
					}
					
				}
			}

			@Override
			public void noException(Exception e) {
				e.printStackTrace();
			}
		};
		// 添加监听者
		client.addConcurrentListener(listener);
		// 设置请求的订阅通道ID
		client.askForGUID(Config.guid);
		// 启动后台线程， 注意这里不会阻塞， 主线程不能退出
		client.start();
	}
}
