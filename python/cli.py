#!/usr/bin/env python
import rabbitcoat


####################
#
# MeatSpace Adapter
#
####################


class rabbit_coat_meat_adapter():
    _server    = None
    _user      = None
    _password  = None
    _vhost     = None
    _timeout   = None
    _in_queue  = None
    _out_queue = None
    _message   = "Ping!"
    _callback  = rabbitcoat.default_callback
    _conf      = rabbitcoat.default_configuration_file

    default_sender = True


    def readConfigurationFile(self,opt):
        

        for opt, arg in opts:
            if opt in ("-S", "--sender"):
                message  = rabbitcoat.default_message
            elif opt in ("-R", "--receiver"):
                callback  = rabbitcoat.default_callback
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
        #self.connection.close()



    def usage(self):
        print 'wrapper.py [--help/-h] [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <in_queue>] [-Q <out_queue>] [-m <message>] [-t <timeout>] [-f <configuration file>]'
        sys.exit()
	
        
    def __init__(self,argv):
        try:
            opts, args = getopt.getopt(argv,"SRhs:u:p:v:q:Q:m:t:c:f:?", #check if that '?' is working
                                       ["receiver","help","sender",
                                        "server=","user=","password=",
                                        "vohst=","in_queue=",
                                        "out_queue=","conf=",
                                        "configuration=",
                                        "configurationFile=",
                                        "message=","timeout="])
        except getopt.GetoptError:
            usage()    
    
            #	    server, user, password, vhost, timeout, in_queue, out_queue, message, callback = None
        isSender = self.default_sender
        for opt, arg in opts:
            if opt in ("-h","-?","--help"):
                usage()
            elif opt in ("-S", "--sender"):
                _isSender = True
            elif opt in ("-R", "--receiver"):
                _isSender = False
            elif opt in ("-s", "--server"):
                _server = arg
            elif opt in ("-u", "--user"):
                _user = arg
            elif opt in ("-p", "--password"):
                _password = arg
            elif opt in ("-v", "--vhost"):
                _vhost = arg
            elif opt in ("-q", "--in_queue"):
                _in_queue = arg
            elif opt in ("-Q", "--out_queue"):
                _out_queue = arg
            elif opt in ("-m", "--message"):
                _message = arg
            elif opt in ("-t", "--timeout"):
                _timeout = arg
            elif opt in ("-f","-c","--conf","--configuration","--configurationfile"):
                _confFile = arg
            else:
                usage()
	
	    
        rabbit = rabbitcoat(_confFile,_server,_user,_password,_vhost,_timeout,_in_queue,_out_queue,_message,_callback)
            

        if (self._callback is None):
            print "Started in Sender mode"
            isSender = True 
        else:
            if (self._message is None):
                print "Started in Receiver mode"
                isSender = False
                


        if (isSender) :
            rabbit.Send(rabbit.out_queue, rabbit.message)
        else :
            rabbit.Receive(rabbit.in_queue)
            


	        
	        


if __name__ == "__main__":
    rabbit_coat_meat_adapter(sys.argv[1:])
    #main(sys.argv[1:])
