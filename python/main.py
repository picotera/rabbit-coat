#!/usr/bin/env python
import pika, os, uuid, logging,sys, getopt
logging.basicConfig()
import time
import json
import rabbitcoat

import threading

import helper
# Parse CLODUAMQP_URL (fallback to localhost)

##urltext = 'amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau'
##furl = os.environ.get('CLOUDAMQP_URL', urltext)#'amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau')
###http://activemq-domainname.rhcloud.com/demo/message/OPENSHIFT/DEMO?type=topic

def usage():
    print 'wrapper.py [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <queue>] [-m <message>] [-t <timeout>]'
    sys.exit()
	
def main(argv):
    
    try:
        opts, args = getopt.getopt(argv,"SRhs:u:p:v:q:Q:m:t:",
                                   ["receiver""sender","server=",
                                    "user=","password=","vohst=",
                                    "in_queue=","out_queue=",
                                    "message=", "timeout="])
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

    #rabbit = RabbitWrapper(server, user, password, vhost, timeout)
    
    #rabbit.run()
    
    config = 'settings.ini'
    
    if in_queue != None:
        receiver = rabbitcoat.RabbitReceiver(config, in_queue, print_callback)
        receiver.run()
    elif out_queue != None and message != None:
        sender = rabbitcoat.RabbitSender(config, out_queue)
        sender.Send({'name' : message})
        
    
if __name__ == "__main__":
    main(sys.argv[1:])
