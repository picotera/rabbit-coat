#!/usr/bin/env python
import pika, os, uuid, logging,sys, ConfigParser
logging.basicConfig()



def validate(value,current,default):
    if ((value is None) or (value == "")):
        if ((current is None) or (current == "")):
            return default
        else:
            return current
    else:
        return value


class rabbitcoat(object):
    
    ####################
    #   Defaults
    ####################

    default_server  = "some server" #"turtle.rmq.cloudamqp.com"
    default_user = "some user" #"qddxpjau"
    default_password = "some password" #"SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg"
    default_vhost = default_user
    default_timeout = 5

    default_queue = "TEST"
    default_message ="Default Test Message"
    default_configuration_file = "rabbitcoat.conf"

    def default_callback(self,ch, method, properties, body):
        print "Receiever: Received message with:"
        #if (properties.correlation_id is None)):
        #    print "\t correlation ID: %s" % properties.corr_id
        if (not (body is None)):
            print "\t body: %r" % (body)
            self.response = body
        else:
            print "\t an empty body"
            self.response = "<EMPTY>" 
        ch.basic_ack(delivery_tag = method.delivery_tag)



    ####################
    #  Initializers
    ####################

    def __init__(self,conf=None,server=None, user=None, password=None, vhost=None, timeout=None, in_queue=None, out_queue=None, message=None, callback=None):
        self.server = self.user = self.password = self.vhost = self.timeout = self.message = self.in_queue = self.out_queue = self.callback = self.response_function = None

        self.readConfigurationFile(conf)
        self.set_infrastructure(server, user, password, vhost, timeout)
        self.set_action_parameters(in_queue, out_queue, message, callback)
        self.init_infrastructure()



    def readConfigurationFile(self,conf=None):
        if (conf is None):
            configFile = self.default_configuration_file
        else:
            configFile = conf

        config = ConfigParser.SafeConfigParser(allow_no_value=True)
        config.read(configFile)
        

        self.server    = validate(config.get('infrastructure', 'server'),self.server,self.default_server)
        self.user      = validate(config.get('infrastructure', 'user')    ,self.user, self.default_user)
        self.password  = validate(config.get('infrastructure', 'password'),self.password,self.default_password)
        self.vhost     = validate(config.get('infrastructure', 'vhost')   ,self.vhost,self.default_vhost)
        self.timeout   = validate(config.get('infrastructure', 'timeout') ,self.timeout,self.default_timeout)
        
        self.in_queue  = validate(config.get('receive parameters', 'in_queue'),self.in_queue,self.default_queue)
        self.callback  = validate(config.get('receive parameters', 'callback'),self.callback,self.default_callback)

        self.out_queue = validate(config.get('send parameters', 'out_queue')  ,self.out_queue,self.default_queue)
        self.message   = validate(config.get('send parameters', 'message')    ,self.message,self.default_message)

        self.response_function =  validate(config.get('response parameters', 'response_function'),
                                           self.response_function, self.default_response)
 
        

        #print self.server    
        #print self.user
        #print self.password  
        #print self.vhost     
        #print self.timeout   
        #print "---"                
        #print self.in_queue  
        #print self.callback 
        #print "---"               
        #print self.out_queue 
        #print self.message
        #print "---"
        #print self.response_function





    def set_infrastructure(self,server=None, user=None, password=None,vhost=None,timeout=None):
        self.server   = validate(server,self.server,self.default_server)
        self.user     = validate(user,self.user,self.default_user)
        self.password = validate(password,self.password,self.default_password)
        self.vhost    = validate(vhost,self.vhost,self.default_vhost)
        self.timeout  = validate(timeout,self.timeout,self.default_timeout)

        

    def init_infrastructure(self):
        ampq_url = "amqp://%s:%s@%s/%s" % (self.user,self.password,self.server,self.vhost)
        url = os.environ.get('CLOUDAMQP_URL',ampq_url)#urltext)#
	
        params = pika.URLParameters(url)
        params.socket_timeout = self.timeout
        self.connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        self.channel = self.connection.channel() # start a channel



    def set_action_parameters(self, in_queue=None, out_queue=None, message=None, callback=None):
        
        print "Action parameters:"
        self.message = validate(message,self.message,self.default_message)
        print "[.] message=%s" % self.message

        self.in_queue = validate(in_queue,self.in_queue,self.default_queue)
        print "[.] in queue=%s" % self.in_queue

        self.out_queue = validate(out_queue,self.out_queue,self.default_queue)
        print "[.] out queue=%s" % self.out_queue

        self.callback = validate(callback,self.callback,self.default_callback)
        print "[.] callback=%s" % self.callback


    ####################
    #   Validators
    ####################

    def validate(value,current,default):
        if (value is None):
            if (current is None):
                return default
            else:
                return current
        else:
            return value
#
#
#    def validate_message(self,message):
#        if (message is None):
#            if (self.message is None):
#                return self.default_message
#            else:
#                return self.message
#        else:
#            return message
#
#
#    def validate_in_queue(self,queue):
#        if (queue is None):
#            if (self.in_queue is None):
#                return self.default_queue
#            else:
#                return self.in_queue
#        else:
#            return queue
#
#
#    def validate_out_queue(self,queue):
#        if (queue is None):
#            if (self.out_queue is None):
#                return self.default_queue
#            else:
#                return self.out_queue
#        else:
#            return queue
#
#        
#    def validate_callback(self,callback):
#        if (callback is None):
#            if (self.callback is None):
#                return self.default_callback
#            else:
#                return self.callback
#        else:
#            return callback
#

    ####################
    #
    #   Simple APIs
    #
    ####################



    def Send(self,queue=None, message=None,corr_id=str(uuid.uuid4()),reply_to_queue=None):
        queue          = validate(queue,self.out_queue,self.default_queue)
        message        = validate(message,self.message,self.default_message)
        reply_to_queue = validate(reply_to_queue,self.in_queue,self.default_queue)
        

        #this may be not so good performance-wize since you constantly open and close the connection
        self.channel.queue_declare(queue,durable=True) # Declare a queue
        # send a message
        self.channel.basic_publish(exchange='', 
                              routing_key=queue, 
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode = 2, # make message persistent
                                  correlation_id = corr_id,
                                  reply_to = reply_to_queue,
                              ))
        print "Sender: Produced message with:"
        print "\t correlation ID: %s" % corr_id
        print "\t body: %s" % message
        self.connection.close()


    def Receive(self,queue=None, callback=None,read_repeatedly=False):
        queue    = validate(queue,self.in_queue,self.default_queue)
        callback = validate(callback,self.callback,self.default_callback)
        channel = self.channel

        #this may be not so good performance-wize since you constantly open and close the connection
        channel.queue_declare(queue,durable=True) # Declare a queue
        self.response = None
        print "Receiver: starting to consume messeges"
        # set up subscription on the queue
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback,
                              queue,
                              no_ack=read_repeatedly)
        
        
        while self.response is None:
            self.connection.process_data_events()
        
        #self.connection.close()


    ####################
    #
    # RPC Functionnality
    #
    ####################


    def default_response(self,body):
        return "response: got %s" % str(body)

    def setResponseFunction(self,function=None):
        self.response_function = validate(function,self.response_function,self.default_response)



    def Respond(self, ch, method, properties, body):
        response = self.response_function(body)

        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         body=str(response),
                         properties=pika.BasicProperties(
                             correlation_id = properties.correlation_id,
                         ))
        ch.basic_ack(delivery_tag = method.delivery_tag)



    def startRespnseServer(self,response_function=None):
        self.setResponseFunction(response_function)
        self.Receive(callback=Respond,read_repeatedly=True)



    def ask(self,send_queue=None, reply_to_queue=None,message=None,callback=None,corr_id=str(uuid.uuid4())):
        self.Send(send_queue, message,corr_id,reply_to_queue)
        self.Receive(reply_to_queue, callback)


#
#####################
##
## MeatSpace Adapter
##
#####################
#
#
#def usage():
#    print 'wrapper.py [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <in_queue>] [-Q <out_queue>] [-m <message>] [-t <timeout>]'
#    sys.exit()
#
#
#
#	
#def main(argv):
#    try:
#        opts, args = getopt.getopt(argv,"SRhs:u:p:v:q:Q:m:t:",
#                                   ["receiver""sender","server=",
#                                    "user=","password=","vohst=",
#                                    "in_queue=","out_queue=",
#                                    "message=","timeout="])
#    except getopt.GetoptError:
#        usage()    
#
#
#    server = user = password = vhost = timeout = in_queue = out_queue = message = callback = None
#    for opt, arg in opts:
#        if opt == '-h':
#            usage()
#        elif opt in ("-S", "--sender"):
#            message  = rabbitcoat.default_message
#        elif opt in ("-R", "--receiver"):
#            callback  = rabbitcoat.default_callback
#        elif opt in ("-s", "--server"):
#            server = arg
#        elif opt in ("-u", "--user"):
#            user = arg
#        elif opt in ("-p", "--password"):
#            password = arg
#        elif opt in ("-v", "--vhost"):
#            vhost = arg
#        elif opt in ("-q", "--in_queue"):
#            in_queue = arg
#        elif opt in ("-Q", "--out_queue"):
#            out_queue = arg
#        elif opt in ("-m", "--message"):
#            message = arg
#        elif opt in ("-t", "--timeout"):
#            timeout = arg
#        else:
#            usage()
#
#    
#    rabbit = rabbitcoat(server,user,password,vhost,timeout,in_queue,out_queue,message,callback)
#    rabbit.run();
#        
#        

def main(argv):
    rabbit = rabbitcoat()
    rabbit.Receive()
    #print "testers unimplemented *yet*- use cli.py"


if __name__ == "__main__":
    main(sys.argv[1:])
