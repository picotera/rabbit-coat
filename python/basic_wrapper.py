#!/usr/bin/env python
import pika, os, logging, sys, getopt
logging.basicConfig()

# Parse CLODUAMQP_URL (fallback to localhost)

url = os.environ.get('CLOUDAMQP_URL', 'amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau')
#http://activemq-domainname.rhcloud.com/demo/message/OPENSHIFT/DEMO?type=topic

default_server  = "turtle.rmq.cloudamqp.com"
default_user = "qddxpjau"
default_password = "SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg"
default_vhost = default_user
default_queue = "DEMO"
default_message ="Ding!"
default_timeout = 5
default_sender = True


def usage():
    print 'wrapper.py [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <queue>] [-m <message>] [-t <timeout>]'
    sys.exit()

# create a function which is called on incoming messages
def callback(ch, method, properties, body):
    print " [x] Received %r" % (body)


def run(isSender, server, user, password, vhost,  queue, message,timeout):
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



    channel.queue_declare(queue,durable=True) # Declare a queue


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

    (isSender,server,user,password,vhost,queue,message,timeout) = (None,None,None,None,None,None,None,None)
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("-S", "--sender"):
            isSender  = True
        elif opt in ("-R", "--receiver"):
            isSender  = False
        elif opt in ("-s", "--server"):
            server = arg
        elif opt in ("-u", "--user"):
            user = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-v", "--vhost"):
            vhost = arg
        elif opt in ("-q", "--queue"):
            queue = arg
        elif opt in ("-m", "--message"):
            message = arg
        elif opt in ("-t", "--timeout"):
            timeout = arg



    run(isSender,server,user,password,vhost,queue,message,timeout)

        
        

if __name__ == "__main__":
    main(sys.argv[1:])
    


