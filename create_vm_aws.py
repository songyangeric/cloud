#-----------------------
# Author: Eric Song 
#-----------------------

import re, logging, getopt, sys, time
from boto3 import *
from ec2_operations import *


def start(args):
    try:
        ec2obj = ec2_operations(args['config'])
        instance_id = ec2obj.create_instance(args['template'], args['name'])
        instance_state = ec2obj.get_instance_running_status(args['name'])
        if instance_state != 'running':
            raise SystemError('Creating ddve on aws failed.')
    
        disk_count = int(args.get('disk_count', 1))
        disk_size = int(args.get('disk_size', 100))
        disk_type = args.get('disk_type', 'st1')
        for i in range(0, disk_count):
            volume_name = args['name'] + '_data' + str(i+1)
            if disk_type == 'st1' and disk_size < 500:
               # Make sure 'st1' data disk is larger than 500GiB
               disk_size = 500
            volume_id = ec2obj.create_volume(volume_name, size = 200, volume_type = disk_type)
            print "volume %s created" % volume_name
            print "volume size %s GiB" % "200"
    
            device_name = '/dev/xvd%s' % chr(98+i)  # index starts from 'xvdb'
            print "device name %s" % device_name
            ec2obj.attach_volume(args['name'], volume_id, device_name)
    
        instance_ip = ec2obj.get_instance_private_ip(args['name'])
        print "Instance private ip: %s" % instance_ip
        print "Instance successfully deployed."
    except Exception as e:
        print "%s: %s" % (getattr(type(e), '__name__'), e)

def usage():
    print """\n Usage: python %s --config <config_file> --template <ami name> --name <instance name> [--disk_type <data disk type>] [--disk_count <data disk num>] [--disk_size <disk size in GiB>]
          """ % sys.argv[0]

try:
    opts, args = getopt.gnu_getopt(sys.argv[1:], 'h', ['config=', 'template=', 'name=', 'disk_type=', 'disk_count=', 'disk_size=', 'help'])
    if len(opts) <= 0:
        usage()
        raise ValueError('No parameters given, please check.')
    kargs = {}
    for arg, value in opts:
        if arg == '-h' or arg == '--help':
            usage()
            exit(2)
        elif arg == '--config' or arg == '--template' or arg == '--name' or arg =='--disk_type' or arg == '--disk_count' or '--disk_size':
            kargs[arg.lstrip('-')] = value 
        else:
            usage()
            raise ValueError("Unsupported parameter format.")
except getopt.GetoptError as e:
    print "Parsing argument encounters %s : %s" % (getattr(type(e), '__name__'), e)
    sys.exit(2)

if kargs.get('config', None) is None or kargs.get('template', None) is None:
    raise ValueError("Either config or template option is absent, please check.")

# create instance
start(kargs)
