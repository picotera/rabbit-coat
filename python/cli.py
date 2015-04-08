#!/usr/bin/env python
import rabbitcoat 
logging.basicConfig()


####################
#
# MeatSpace Adapter
#
####################


def usage():
    print 'wrapper.py [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <in_queue>] [-Q <out_queue>] [-m <message>] [-t <timeout>] [-f <configuration file>]'
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

    for opt, arg in opts:
        if opt in ("-h","-?","--help"):
            usage()
        elif opt in ("-f", "--conf","--configuration","--configurationfile"):
            readConfigurationFile(opt)


    confFile = server = user = password = vhost = timeout = in_queue = out_queue = message = callback = None
    for opt, arg in opts:
        if opt in ("-S", "--sender"):
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
        elif opt in ("-f", "--conf"):
            confFile = arg
        else:
            usage()

    
    server, user, password, vhost = 
    rabbit = RabbitWrapper(server,user,password,vhost,timeout,in_queue,out_queue,message,callback)
    rabbit.run();
        
        

if __name__ == "__main__":
    main(sys.argv[1:])
