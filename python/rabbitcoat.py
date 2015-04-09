#!/usr/bin/env python
import pika, os, uuid, logging,sys, ConfigParser
logging.basicConfig()



def validate(value,current,default=None):
    if ((value is None) or (value == "")):
        if ((not (default is None)) and ((current is None) or (current == ""))):
            return default
        else:
            return current
    else:
        return value

#class rabbitcoat(Rabbit): 


class RabbitFrame(object):
    default_configuration_file = "rabbitcoat.conf"
   def __init__(self,conf=None,server=None, user=None, password=None, vhost=None, timeout=None):
        self.server = self.user = self.password = self.vhost = self.timeout = None
        self.__loadConfiguration(conf)
        self.__initInfrastructure(server, user, password, vhost, timeout)


    def __loadConfiguration(self,conf=None):
        if (conf is None):
            configFile = self.default_configuration_file
        else:
            configFile = conf

        config = ConfigParser.SafeConfigParser(allow_no_value=True)
        config.read(configFile)
        
        self.server    = validate(config.get('infrastructure', 'server')      ,self.server)
        self.user      = validate(config.get('infrastructure', 'user')        ,self.user)
        self.vhost     = validate(config.get('infrastructure', 'vhost')       ,self.vhost    ,self.user)
        self.password  = validate(config.get('infrastructure', 'password')    ,self.password)
        self.timeout   = validate(config.get('infrastructure', 'timeout')     ,self.timeout  ,5)



    def __setInfrastructure(self,server=None, user=None, password=None,vhost=None,timeout=None):
        self.server   = validate(server,self.server)
        self.user     = validate(user,self.user)
        self.password = validate(password,self.password)
        self.vhost    = validate(vhost,self.vhost)
        self.timeout  = validate(timeout,self.timeout)



    def __initInfrastructure(self,server=None, user=None, password=None,vhost=None,timeout=None):
        self.__setInfrastructure(server, user, password, vhost, timeout) #override config file if the user wants
        ampq_url = "amqp://%s:%s@%s/%s" % (self.user,self.password,self.server,self.vhost)
        url = os.environ.get('CLOUDAMQP_URL',ampq_url)
	
        params = pika.URLParameters(url)
        params.socket_timeout = self.timeout
        self.connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        self.channel = self.connection.channel() # start a channel




class RabbitSender(RabbitFrame):
    
    def __init__(self, config, queue, reply_to=None):
        RabbitChannel.__init__(self, config)
        
        self.queue = queue
        self.channel.queue_declare(queue, durable=True)
        self.reply_to = reply_to       

    def Send(self,data=None,corr_id=str(uuid.uuid4()),reply_to_queue=None,channel=None):
        message        = json.dumps(data)
        reply_to_queue = validate(reply_to_queue,self.reply_to)

        channel = validate(channel,self.channel)

        channel.queue_declare(queue,durable=True) # Declare a queue
        # send a message
        channel.basic_publish(exchange='', 
                              routing_key=self.queue, 
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode = 2, # make message persistent
                                  correlation_id = corr_id,
                                  reply_to = reply_to_queue,
                              ))
        print "Sender: Produced message with:"
        print "\t correlation ID: %s" % corr_id
        print "\t body: %s" % message



class RabbitReceiver(RabbitFrame, threading.Thread):

     def __init__(self, config, queue, callback,read_repeatedly=False):
        RabbitChannel.__init__(self, config)
        threading.Thread.__init__(self, name='RabbitReceiver %s' %queue)
        
        self.queue = validate(queue,self.in_queue)
        self.channel.queue_declare(queue,durable=True) # Declare a queue

        self.read_repeatedly = read_repeatedly        
        self.callback = validate(callback,self.callback)



    def run(self):
        channel = self.channel
        ''' Bind a callback to the queue '''
        print "Receiver: starting to consume messeges"
        
        # set up subscription on the queue
        channel.basic_qos(prefetch_count=1)
        
        channel.basic_consume(self.callback,
                              self.queue,
                              no_ack=self.read_repeatedly)
        
        channel.start_consuming()


    def Receive(self,queue=None, callback=None,read_repeatedly=False):
        self.__init__(queue, callback,read_repeatedly)
        self.run()




class RabbitResponder(RabbitReceiver,RabbitSender):

     def __init__(self, config, inbound_queue, outbound_queue, response_function,read_repeatedly=False):
         RabbitReceiver.__init__(self, config, queue, Respond,read_repeatedly)
         self.response_function = validate(response_function,self.response_function,self.default_response)


    def Respond(self, ch, method, properties, body):
        response = self.response_function(body)
        RabbitSender.__init__(self, config, properties.reply_to, inbound_queue)        
        self.Send(self,data=response,corr_id=properties.correlation_id,channel=ch):
        ch.basic_ack(delivery_tag = method.delivery_tag)


    def default_response(self,body):
        return "response: got %s" % str(body)


    def Start(self)
        self.run()



class RabbitRequester(RabbitSender,RabbitReceiver):

    def __init__(self, config, outbound_queue, callback, inbound_queue, read_repeatedly=False):
        RabbitSender.__init__(self, config, outbound_queue, inbound_queue)
        RabbitReceiver.__init__(self, config, queue, callback, read_repeatedly)


    def ask(self,send_queue=None, reply_to_queue=None,message=None,callback=None,corr_id=str(uuid.uuid4())):
        self.Send(send_queue, message,corr_id,reply_to_queue)
        self.Receive(reply_to_queue, callback)



def main(argv):
    #rabbit = rabbitcoat()
    #rabbit.Receive()
    print "testers unimplemented *yet*- use cli.py"


if __name__ == "__main__":
    main(sys.argv[1:])
