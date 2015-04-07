package jbossews;

import com.rabbitmq.client.*;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import javax.jws.WebService;
import javax.jws.WebMethod;

@WebService
public class AMPQConnectorService {
	
	public static final String  default_server  = "turtle.rmq.cloudamqp.com";
	private static final String default_user = "qddxpjau";
	private static final String default_password = "SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg";
	private static final String default_vhost = default_user;
	public static final String  default_queue = "DEMO";
	public static final String  default_message ="Ding!";
	public static final int     default_timeout = 30000;
	public static final Boolean default_sender = true;

	
	

			

//			#function which is called on incoming messages
//			def callback(ch, method, properties, body):
//			    print " [x] Received %r" % (body)

/*
			def run():
			    if (isSender is None):
			        isSender = default_sender
			    if (server is None):
			        server = default_server
			    if (user is None):
			        user = default_user
			    if (vhost is None):
			        vhost = default_vhost
			    if (password is None):
			        password = default_password
			    if (queue is None):
			        queue = default_queue
			    if (message is None):
			        message = default_message
			    if (timeout is None):
			        timeout = default_timeout

			#    body="?body=%s" % message
			#    body="&body=%s" % message
			    #ampq_url = "ampq://%s:%s@%s/%s" % (user,password,server,vhost)
			    ampq_url = "ampq://%s:%s@%s/%s" % (user,password,server,vhost)
			    
			    url = os.environ.get('CLOUDAMQP_URL',ampq_url)


			    params = pika.URLParameters(url)
			    params.socket_timeout = timeout
			    connection = pika.BlockingConnection(params) # Connect to CloudAMQP
			    channel = connection.channel() # start a channel



			    channel.queue_declare(queue) # Declare a queue


			    if (isSender) :
			        # send a message
			        channel.basic_publish(exchange='', 
			                              routing_key=queue, 
			                              body=message,
			                              properties=pika.BasicProperties(
			                                  delivery_mode = 2, # make message persistent
			                              ))
			        print "Sender: Produced message: %s" % message
			    else :
			        print "Receiver: starting to consume messeges"
			        # set up subscription on the queue
			        channel.basic_consume(callback,
			                              queue,
			                              no_ack=True)
			        
			        channel.start_consuming() # start consuming (blocks)
			        
			        connection.close()

				
			def main(argv):
			    try:
			        opts, args = getopt.getopt(argv,"SRhs:u:p:v:q:m:t:",["receiver""sender","server=","user=","password=","vohst=","queue=","message=","timeout="])
			    except getopt.GetoptError:
			        usage()    

			    isSender,server,user,password,vhost,queue,message,timeout = true,null,null,null,null,null,null,null;
			    for opt, arg in opts:
			        if opt == '-h':
			            usage()
			        else if  opt in ("-S", "--sender"):
			            isSender  = True
			            		else if  opt in ("-R", "--receiver"):
			            isSender  = False
			            		else if  opt in ("-s", "--server"):
			            server = arg
			            		else if  opt in ("-u", "--user"):
			            user = arg
			            		else if  opt in ("-p", "--password"):
			            password = arg
			            		else if  opt in ("-v", "--vhost"):
			            vhost = arg
			            		else if  opt in ("-q", "--queue"):
			            queue = arg
			            		else if  opt in ("-m", "--message"):
			            message = arg
			            else if opt in ("-t", "--timeout"):
			            timeout = arg



			    run(isSender,server,user,password,vhost,queue,message,timeout)
			    */

	@WebMethod
	public static String run() throws Exception{
			return run(false,null,null,null,null,null,null,-1);
	}
	
	@WebMethod
	public static String run(Boolean isSender, String server, String user, String password, 
			String vhost,  String queue, String message,int timeout) throws Exception {
		
		String retval = "test2!";
		
		if (server == null)   server = default_server;
		if (user == null)     user = default_user;
		if (password == null) password = default_password;
		if (vhost == null)    vhost = default_vhost;
		if (queue == null)    queue   = default_queue;
		if (message == null)  message = default_message;
		if (timeout < 0)      timeout = default_timeout;
		
		
		String uri = System.getenv("CLOUDAMQP_URL");
		
		//default: "amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau;
		if (uri == null) uri = "amqp://"+user+":"+password+"@"+server+"/"+vhost;
		
		
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(uri);

		//Recommended settings
		factory.setRequestedHeartbeat(30);
		factory.setConnectionTimeout(timeout);

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		boolean durable = true;    //durable - RabbitMQ will never lose the queue if a crash occurs
		boolean exclusive = false;  //exclusive - if queue only will be used by one connection
		boolean autoDelete = false; //autodelete - queue is deleted when last consumer unsubscribes

		channel.queueDeclare(queue, durable, exclusive, autoDelete, null);


		String exchangeName = "";
		String routingKey = queue;

		if (isSender) {
			channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
			retval += " [x] Sent '" + message + "'";
		}
		else {
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queue, true, consumer);

			boolean caught = false;
			while (!caught) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				retval += delivaryCallback(delivery);
				caught = true;
			}
		}

		return retval;
	}

	
	private static String delivaryCallback(Delivery delivery) {
		String message = new String(delivery.getBody());
		return " [x] Received '" + message + "'";
	}

	
	public static void usage(String appname) {
	    System.out.printf("%s [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <queue>] [-m <message>] [-t <timeout>]\n",appname);
	    System.exit(1);
	}

	public static void main(String[] args) throws Exception {
		run();
	}

}
