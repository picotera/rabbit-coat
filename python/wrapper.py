#!/usr/bin/env python
import pika, os, uuid, logging,sys, getopt
logging.basicConfig()

# Parse CLODUAMQP_URL (fallback to localhost)

##urltext = 'amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau'
##furl = os.environ.get('CLOUDAMQP_URL', urltext)#'amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau')
###http://activemq-domainname.rhcloud.com/demo/message/OPENSHIFT/DEMO?type=topic


class  RabbitWrapper(object):

    
    ####################
    #   Defaults
    ####################

    default_server  = "turtle.rmq.cloudamqp.com"
    default_user = "qddxpjau"
    default_password = "SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg"
    default_vhost = default_user
    default_queue = "TEST"
    default_message ="Ding!"
    default_timeout = 5
    default_sender = True
    def default_callback(self,ch, method, properties, body):
        print "Receiever: Received message with:"
        #if (properties.corr_id is None)):
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

    def __init__(self,server=None, user=None, password=None, vhost=None, timeout=None, in_queue=None, out_queue=None, message=None, callback=None):
        self.isSender = self.default_sender
        if (callback is None):
            print "Started in Sender mode"
            self.isSender = True 
        else:
            if (message is None):
                print "Started in Receiver mode"
                self.isSender = False
        
        self.set_defaults(server, user, password, vhost, timeout)
        self.set_action_parameters(in_queue, out_queue, message, callback)
        self.init_infrastructure()


    def set_defaults(self,server=default_server, user=default_user, password=default_password,
                     vhost=default_vhost, timeout=default_timeout):
        if (server is None):   server =   self.default_server
        if (user is None):     user =     self.default_user
        if (password is None): password = self.default_password
        if (vhost is None):    vhost =    self.default_vhost
        if (timeout is None):  timeout =  self.default_timeout

        self.server = server
        self.user = user
        self.password = password
        self.vhost = vhost
        self.timeout = timeout
        

    def init_infrastructure(self):
        ampq_url = "amqp://%s:%s@%s/%s" % (self.user,self.password,self.server,self.vhost)
        url = os.environ.get('CLOUDAMQP_URL',ampq_url)#urltext)#
	
        params = pika.URLParameters(url)
        params.socket_timeout = self.timeout
        self.connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        self.channel = self.connection.channel() # start a channel


    def set_action_parameters(self, in_queue=None, out_queue=None, message=None, callback=None):
        self.message = self.in_queue = self.out_queue = self.callback = None
        
        print "Action parameters:"
        self.message = self.validate_message(message)
        print "[.] message=%s" % self.message

        self.in_queue = self.validate_in_queue(in_queue)
        print "[.] in queue=%s" % self.in_queue

        self.out_queue = self.validate_out_queue(out_queue)
        print "[.] out queue=%s" % self.out_queue


    ####################
    #   Validators
    ####################

    def validate_message(self,message):
        if (message is None):
            if (self.message is None):
                return self.default_message
            else:
                return self.message
        else:
            return message


    def validate_in_queue(self,queue):
        if (queue is None):
            if (self.in_queue is None):
                return self.default_queue
            else:
                return self.in_queue
        else:
            return queue


    def validate_out_queue(self,queue):
        if (queue is None):
            if (self.out_queue is None):
                return self.default_queue
            else:
                return self.out_queue
        else:
            return queue

        
    def validate_callback(self,callback):
        if (callback is None):
            if (self.callback is None):
                return self.default_callback
            else:
                return self.callback
        else:
            return callback


    ####################
    #
    #   Simple APIs
    #
    ####################


    def run(self):
        if (self.isSender) :
            #self.channel.queue_declare(self.out_queue,durable=True) # Declare a queue
            self.Send(self.out_queue, self.message)
        else :
            #self.channel.queue_declare(self.in_queue,durable=True) # Declare a queue
            self.Receive(self.in_queue)
        #self.connection.close()


    def Send(self,queue=None, message=None,corr_id=str(uuid.uuid4()),reply_to_queue=None):
        queue = self.validate_out_queue(queue)
        message = self.validate_message(message)
        reply_to_queue = self.validate_in_queue(queue)
        

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


    def Receive(self,queue=None, callback=None):
        queue = self.validate_in_queue(queue)
        callback = self.validate_callback(callback)
        channel = self.channel

        #this may be not so good performance-wize since you constantly open and close the connection
        channel.queue_declare(queue,durable=True) # Declare a queue
        self.response = None
        print "Receiver: starting to consume messeges"
        # set up subscription on the queue
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback,
                              queue,
                              no_ack=False)
        
        
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
        if (function is None):
            function = self.default_response
        self.response_function = function



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
        setResponseFunction(response_function)
        Respond(callback=Respond)



    def ask(self,send_queue=None, reply_to_queue=None,message=None,callback=None,corr_id=str(uuid.uuid4())):
        self.Send(send_queue, message,corr_id,reply_to_queue)
        self.Receive(reply_to_queue, callback)



####################
#
# MeatSpace Adapter
#
####################


def usage():
    print 'wrapper.py [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <queue>] [-m <message>] [-t <timeout>]'
    sys.exit()



	
def main(argv):
    try:
        opts, args = getopt.getopt(argv,"SRhs:u:p:v:q:Q:m:t:",
                                   ["receiver""sender","server=",
                                    "user=","password=","vohst=",
                                    "in_queue=","out_queue=",
                                    "message=","timeout="])
    except getopt.GetoptError:
        usage()    

    server = user = password = vhost = timeout = in_queue = out_queue = message = callback = None
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("-S", "--sender"):
            message  = RabbitWrapper.default_message
        elif opt in ("-R", "--receiver"):
            callback  = RabbitWrapper.default_callback
        elif opt in ("-s", "--server"):
            server = arg
        elif opt in ("-u", "--user"):
            user = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-v", "--vhost"):
            vhost = arg
        elif opt in ("-q", "--in_queue"):
            in_queue = arg
        elif opt in ("-Q", "--out_queue"):
            out_queue = arg
        elif opt in ("-m", "--message"):
            message = arg
        elif opt in ("-t", "--timeout"):
            timeout = arg
        else:
            usage()


    rabbit = RabbitWrapper(server,user,password,vhost,timeout,in_queue,out_queue,message,callback)
    rabbit.run();
        
        

if __name__ == "__main__":
    main(sys.argv[1:])
