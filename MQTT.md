```
package com.hlhz.mqtt;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
//向生产者注册的信息
public class Thread1 extends Thread{
	private String host = "";//tcp://192.168.0.7:1883
	private String topic = "";//source
    private String clientid = "";//huzy
//    private MqttClient client;
//    private MqttConnectOptions options;
    private String userName = "";    //非必须
    private String passWord = "";  //非必须 
    private String port = "";  //非必须 
    private int i;
    

	public Thread1(String host, String topic, String clientid, String userName, String passWord, String port, int i) {
		super();
		this.host = host;
		this.topic = topic;
		this.clientid = clientid;
		this.userName = userName;
		this.passWord = passWord;
		this.port = port;
		this.i = i;
	}
	
	@SuppressWarnings("unused")
	@Override
	public void run() {
        try {
            // host为主机名，clientid即连接MQTT的客户端ID，一般以唯一标识符表示，MemoryPersistence设置clientid的保存形式，默认为以内存保存
        	MqttClient client = new MqttClient("tcp://"+host+":"+port, clientid, new MemoryPersistence());
            // MQTT的连接设置
        	MqttConnectOptions options = new MqttConnectOptions();
            // 设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，设置为true表示每次连接到服务器都以新的身份连接
            options.setCleanSession(false);
            if(StringUtils.isNotEmpty(userName)){
            	// 设置连接的用户名
                options.setUserName(userName);
            }
            if(StringUtils.isNotEmpty(passWord)){
            	// 设置连接的密码
                options.setPassword(passWord.toCharArray());
            }
            
            // 设置超时时间 单位为秒
            options.setConnectionTimeout(10);
            // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送个消息判断客户端是否在线，但这个方法并没有重连的机制
            options.setKeepAliveInterval(5);
            options.setCleanSession(false);
            // 设置回调
            client.setCallback(new MqttCallback() {
            	
            	//断线重连
                public void reConnect() throws Exception {
                    if(null != client) {
                        client.connect(options);
                        //订阅消息，重连之后一定要订阅，否则即使重连成功也获取不到消息
                        int[] Qos  = {0};
                        String[] topic1 = {topic};
                        client.subscribe(topic1, Qos);
                    }
                }

                public void connectionLost(Throwable cause) {
                    // 连接丢失后，一般在这里面进行重连
                    System.out.println("当前服务器："+host+"连接断开，可以做重连");
                    while (true) {
                        if (!client.isConnected()) {
                            System.out.println("当前服务器："+host+"***** client to connect *****");
                            try {
                                //这个是30秒后重连
                                Thread.sleep(3000);
                                reConnect();
                            } catch (Exception e) {
                                e.printStackTrace();
                                continue;
                            }
                        }
                        if (client.isConnected()) {
                            System.out.println("当前服务器："+host+"***** connect success *****");
                            break;
                        }
                    }
                }

                public void messageArrived(String topic, MqttMessage message) throws Exception {
                	String content =new String(message.getPayload());
                	System.out.println("当前服务器："+host+"接收消息主题 : " + topic+"接收消息内容 : " + content);
//                	AllDBService.rddbs("7", RDDBindex.default_rddb).set(RedisIndex.one, "123", "test");
                	RedisData redis=new RedisData(content);
                	redis.start();
                	Thread t=Thread.currentThread();
                	System.out.println("线程："+t.getName()+t.getId()+"；"+"是否处于活跃状态："+t.isAlive());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("当前服务器："+host+"deliveryComplete---------"+ token.isComplete());
                }
                

            });
            MqttTopic top = client.getTopic(topic);
            //setWill方法，如果项目中需要知道客户端是否掉线可以调用该方法。设置最终端口的通知消息
//遗嘱        options.setWill(topic, "close".getBytes(), 2, true);
            client.connect(options);
            //订阅消息
            int[] Qos  = {0};
            String[] topic1 = {topic};
            client.subscribe(topic1, Qos);

        } catch (Exception e) {
        	String filePath ="d:\\mqtt_log\\" ;
    		File file1 = new File(filePath);
    		if (!file1.exists()) {
    			file1.mkdirs();
    		}
    		FileOutputStream outSTr = null;
    		BufferedOutputStream Buff = null;
    		File file = new File(filePath+"log.txt");
    		Thread t=Thread.currentThread();
    		String con=host+"线程："+t.getName()+t.getId()+"；"+"是否处于活跃状态："+t.isAlive();
			if (!file.exists()) {
				try {
					file.createNewFile();
					outSTr = new FileOutputStream(file);
					Buff = new BufferedOutputStream(outSTr);
					Buff.write(con.getBytes());
					Buff.flush();
					Buff.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
            e.printStackTrace();
        }
    }
	
	public int getI() {
		return i;
	}
	public void setI(int i) {
		this.i = i;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getClientid() {
		return clientid;
	}
	public void setClientid(String clientid) {
		this.clientid = clientid;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassWord() {
		return passWord;
	}
	public void setPassWord(String passWord) {
		this.passWord = passWord;
	}
    
}

```

