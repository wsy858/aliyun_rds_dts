package dts;

import java.util.List;

import util.Config;

import com.aliyun.drc.client.message.DataMessage.Record;
import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;

public class MainClass {
	// 消费者
	private ClusterClient client = null;

	public void initClent() {
		// 创建一个context
		RegionContext context = new RegionContext();
		// 运行SDK的服务器是否使用公网IP连接DTS
		context.setUsePublicIp(true);
		// 用户accessKey secret
		context.setAccessKey(Config.accessKey);
		context.setSecret(Config.secret);
		// 创建消费者
		client = new DefaultClusterClient(context);
		try {
			// 添加监听者
			client.addConcurrentListener(listener);
			// 设置请求的订阅通道ID
			client.askForGUID(Config.guid);
			// 启动后台线程， 注意这里不会阻塞， 主线程不能退出
			client.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 创建订阅监听者listener
	ClusterListener listener = new ClusterListener() {
		// 这个函数主要用于监听订阅到的增量数据，然后对订阅到的数据进行消费
		@Override
		public void notify(List<ClusterMessage> messages) throws Exception {
			for (ClusterMessage message : messages) {
				// 获取订阅到的数据
				Record record = message.getRecord();
				try {
					//处理订阅到的消息
					boolean finished = messageHandler(record);
					// 消费完数据后向DTS汇报ACK，必须调用
					if(finished){
					   message.ackAsConsumed();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void noException(Exception e) {
			e.printStackTrace();
		}
	};
	
	
	//消息处理
	public boolean messageHandler(Record record){
		boolean finished = false;
		MessageHandler handler = new MessageHandler();
		switch (record.getOpt()) {
		case INSERT:
			finished = handler.handleInsert(record);
			break;
		case UPDATE:
			finished = handler.handleUpdate(record);
			break;
		case DELETE:
			finished = handler.handleDelete(record);
			break;
		case DDL:
			finished = handler.handleDdl(record);
			break;
		default:
			break;
		}
		return finished;
	}
	

	// 主函数
	public static void main(String[] args) throws Exception {
		new MainClass().initClent();
	}

}
