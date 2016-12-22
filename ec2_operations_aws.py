#--------------------
# Author: Eric Song(songyangeric@outlook.com)
#--------------------

import re, logging, getopt, sys, time, os
from boto3 import *

# for detailed instance type info, please visit https://aws.amazon.com/ec2/instance-types/
instance_types = [
        't2.nano', 't2.micro', 't2.small', 't2.medium', 't2.large', 't2.xlarge', 't2.2xlarge',
        'm4.large', 'm4.xlarge', 'm4.2xlarge', 'm4.4xlarge', 'm4.10xlarge', 'm4.16xlarge',
        'm3.medium', 'm3.large', 'm3.xlarge', 'm3.2xlarge',
        'c4.large', 'c4.xlarge', 'c4.2xlarge', 'c4.4xlarge', 'c4.8xlarge',
        'c3.large', 'c3.xlarge', 'c3.2xlarge', 'c3.4xlarge', 'c3.8xlarge',
        'x1.32xlarge', 'x1.16xlarge',
        'r4.large', 'r4.xlarge', 'r4.2xlarge', 'r4.4xlarge', 'r4.8xlarge', 'r4.16xlarge',
        'r3.large', 'r3.xlarge', 'r3.2xlarge', 'r3.4xlarge', 'r3.8xlarge',
        'p2.xlarge', 'p2.8xlarge', 'p2.16xlarge',
        'g2.2xlarge', 'g2.8xlarge',
        'f1.2xlarge', 'f1.16xlarge',
        ]

# for detailed volume type info, please visit http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
volume_types = [
        'gp2', 'io1',
        'st1', 'sc1',
        ]

class ec2_operations:
    
    def __init__(self, config_file):
        ddve_config = self.parse_params(config_file)
        self.aws_access_key_id = ddve_config['aws_access_key_id']
        self.aws_secret_access_key = ddve_config['aws_secret_access_key']
        self.region = ddve_config['region']
        self.subnet = ddve_config.get('subnet', 'cali-priv-sub-1a')
        self.instance_type = ddve_config.get('intance_type', 'm4.large')
        if self.instance_type not in instance_types:
            raise ValueError("Invalid instance type, please check.")
        self.security_group = ddve_config.get('security_group', 'ddve-priv-sg')

        session = Session(aws_access_key_id = self.aws_access_key_id,
                          aws_secret_access_key = self.aws_secret_access_key,
                          region_name = self.region)

        self.ec2_resource = session.resource('ec2')
        self.ec2_client = session.client('ec2')

    def list_instances(self):
        for instance in self.ec2_resource.instances.all():
            print instance.id, instance.instance_type
    
    def parse_params(self, config_file):
        ddve_config = {}
        comment_str = r'^\s*#' # filter comments
        comment_pat = re.compile(comment_str)
        blank_pat = re.compile(r'^\s*$')
        with open(config_file) as file:
            for line in file:
                if blank_pat.search(line) is not None or comment_pat.search(line) is not None:
                    continue
                else:
                    items = line.split('=')
                    ddve_config[items[0].strip()] = items[1].strip()
        if ddve_config['aws_access_key_id'] is None or ddve_config['aws_secret_access_key'] is None or ddve_config['region'] is None:
            raise ValueError("Wrong configuration file given, please check.")
    
        return ddve_config
   
    def get_ami_id_from_tag(self, tag):
        for obj in self.ec2_resource.images.filter(Filters=[{'Name': 'tag:Name', 'Values': [tag]}]):
            return obj.id
        return None

    def get_ami_id_from_ami_name(self, ami_name):
        for obj in self.ec2_resource.images.filter(Filters=[{'Name': 'name', 'Values': [ami_name]}]):
            return obj.id
        return None
        
    def get_instance_id_from_tag(self, tag):
        for obj in self.ec2_resource.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tag]}]):
            return obj.id
        return None

    def get_volume_id_from_tag(self, tag):
        for obj in self.ec2_resource.volumes.filter(Filters=[{'Name': 'tag:Name', 'Values': [tag]}]):
            return obj.id
        return None

    def get_subnet_id(self):
        for obj in self.ec2_resource.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [self.subnet]}]):
            return obj.id
        return None

    def get_availability_zone_from_subnet(self):
        for obj in self.ec2_resource.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [self.subnet]}]):
            return obj.availability_zone
        return None
    
    def get_id_from_ami(self, tag):
        for obj in self.ec2_resource.filter(Filters=[{'Name': 'AmiName', 'Values': [tag]}]):
            return obj.id
        return None
    
    def get_security_group_id(self):
        for obj in self.ec2_resource.security_groups.filter(Filters=[{'Name': 'tag:Name', 'Values': [self.security_group]}]):
            return obj.id
        return None

    def create_volume(self, name, size = 100, volume_type = 'gp2', snapshot_id = None, encrypted = False):
        availability_zone = self.get_availability_zone_from_subnet()
        if volume_type not in volume_types:
            raise ValueError('Invalid volume type %s, please check.' % volume_type)
        volume = None
        try:
            if snapshot_id is None:
                volume = self.ec2_resource.create_volume(AvailabilityZone = availability_zone, 
                                           Size = size, 
                                           VolumeType = volume_type,  
                                           Encrypted = encrypted)
            else:         
                volume = self.ec2_resource.create_volume(AvailabilityZone = availability_zone, 
                                           Size = size, 
                                           VolumeType = volume_type,  
                                           SnapshotId = snapshot_id,
                                           Encrypted = encrypted)
        except Exception as e:
            print 'Creating volume encoutered errors -> %s: %s' % (getattr(type(e), '__name__'), e)
            sys.exit(1)
      
        while volume.state != 'available':
            time.sleep(10)
            volume.load()

        volume_id = volume.id
        self.ec2_resource.create_tags(
            Resources = [volume_id],
            Tags = [{'Key': 'Name', 'Value': name}]
        )

        return volume_id

    def attach_volume(self, instance_name, volume_id, device = None):
        instance = self.get_instance_from_tag(instance_name)
        if device is None:
            volume_num = 0
            volumes = instance.volumes.all()
            for volume in volumes:
                volume_num += 1
            device = '/dev/xvd%s/' % chr(97+volume_num)

        response = instance.attach_volume(VolumeId = volume_id,
                                        Device = device)
        time.sleep(10)
        for volume in instance.volumes.all():
            if volume.id == volume_id:
                return True

        return False

    def detach_volume(self, instance_name, volume_id, force = False):
        instance = self.get_instance_from_tag(instance_name)
        response = instance.detach_volume(VolumeId = volume_id,
                                        Force = force)
        time.sleep(20)
        for volume in instance.volumes.all():
            if volume.id == volume_id:
                return False

        return True

    # Before deletion, the EBS must be detached from the instance
    def delete_volume(self, volume_id):
        self.ec2_client.delete_volume(VolumeId = volume_id)
        
        time.sleep(20)
        for volume in self.ec2_resource.volumes.all():
            if volume.id == volume_id:
                return False

        return True

    def create_instance(self, ami, instance_name):
        security_group_ids = []
        security_group_id = self.get_security_group_id()
        security_group_ids.append(security_group_id)

        subnet_id = self.get_subnet_id()
        availability_zone = self.get_availability_zone_from_subnet()
        
        # first check the tag name and then the ami name
        ami_id = self.get_ami_id_from_tag(ami)
        if ami_id is None:
            ami_id = self.get_ami_id_from_ami_name(ami)
            if ami_id is None:
                raise ValueError("Wrong AMI provided, please check.")

        # block device
#        blockdev = [
#            {
#                'DeviceName': '/dev/xvdb',
#                'Ebs': {
#                          'VolumeSize': 100,
#                          'VolumeType': 'gp2',
#                          'DeleteOnTermination': True,
#                }
#            }]
        try:
            instances = self.ec2_resource.create_instances(ImageId = ami_id, MinCount = 1, MaxCount = 1,
                                         SubnetId = subnet_id,
                                         SecurityGroupIds = security_group_ids,
                                         InstanceType = self.instance_type,
                                         Placement = {
                                             'AvailabilityZone': availability_zone,
                                         },
                                         InstanceInitiatedShutdownBehavior = 'stop',
                                         BlockDeviceMappings = blockdev,
                                         EbsOptimized = True
                                        )
        except Exception as e:
            print 'Creating instance encoutered errors -> %s: %s' % (getattr(type(e), '__name__'), e)
            sys.exit(1)

        instance = instances[0]
        instance.wait_until_running()
        instance.load()

        instance_id = instance.id
        self.ec2_resource.create_tags(
            Resources = [instance_id],
            Tags = [{'Key': 'Name', 'Value': instance_name}]
        )

        return instance_id


    def get_instance_from_tag(self, instance_name):
        for obj in self.ec2_resource.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}]):
            return obj

    def get_instance_running_status(self, instance_name):
        instance = self.get_instance_from_tag(instance_name)
        return instance.state['Name']

    def get_instance_private_ip(self, instance_name):
        instance = self.get_instance_from_tag(instance_name)
        return instance.private_ip_address

    def start_instance(self, instance_name):
        instance = self.get_instance_from_tag(instance_name)
        instance.start()

        instance.wait_until_running()
        if instance_state == 'running':
            print 'Successfully started instance %s' % instance_name
        else:
            print 'Failed to start instance %s' % instance_name

    def stop_instance(self, instance_name, force = False):
        instance = self.get_instance_from_tag(instance_name)
        instance.stop(force)
        instance.wait_until_stopped()

        instance_state = self.get_instance_running_status(instance_name)
        if instance_state == 'stopped':
            print 'Successfully stopped instance %s' % instance_name
        else:
            print 'Failed to stop instance %s' % instance_name

    def terminate_instance(self, instance_name, delete_all_volumes = False):
        instance = self.get_instance_from_tag(instance_name)
        volumes_to_delete = []
        if delete_all_volumes:
            volumes = instance.volumes.all()
            for volume in volumes:
                device_name =  volume.attachments[0]['Device']
                if re.match(r'/dev/xvd', device_name):
                    volumes_to_delete.append(volume.id)
        
        instance.terminate()
        instance.wait_until_terminated()

        if delete_all_volumes:
            for volume_id in volumes_to_delete:
                self.delete_volume(volume_id)
       
        instance_state = self.get_instance_running_status(instance_name)
        if instance_state == 'terminated':
            print 'Successfully terminated instance %s' % instance_name
        else:
            print 'Failed to terminate instance %s' % instance_name

def usage():
    print '''\nUsage: python %s --config <config file>
               --op <create|start|stop|terminate|delete|add_volume|detach_volume|delete_volume|ip|state> 
              [--ami <ami>] "only valid when --op=create is specified" 
               --name <instance name
              [--vol_type <volume type>] "only valid when --op=add_volume is specified"
              [--vol_size <volume size>] "only valid when --op=add_volume is specified"
              [--vol_name <vol_name>] "valid when --op=add_volume|detach_volume|delete_volume"
          ''' % sys.argv[0]

def check_params():
    opts,args = getopt.gnu_getopt(sys.argv[1:], 'h', ['config=', 'ami=', 'op=', 'name=', 'vol_type=', 'vol_size=', 'vol_name=', 'help'])
    if len(opts) <= 1:
        usage()
        sys.exit(2)
    kargs = {}
    for arg, value in opts:
        if arg == '-h' or arg == '--help':
            usage()
            sys.exit(2)
        if arg == '--config' or arg == '--op' or arg == '--ami' or arg == '--name' or arg == '--vol_type' or arg == '--vol_size' or arg == '--vol_name' or arg == '--vol_type':
            kargs[arg.lstrip('-')] = value
        else:
            usage()
            raise ValueError("Unsupported parameter.")

    return kargs

def start(kargs):
    module_path = os.path.dirname(__file__)
    config_file = kargs.get('config', module_path + '/VM_Config_AWS')
    op = kargs.get('op', 'state')
    ami = kargs.get('ami', None)
    if op == 'create' and ami is None:
        raise ValueError('AMI must be specified to create an instance')
    instance_name = kargs.get('name', '')
    if instance_name == '' and op != 'delete_volume':
        raise ValueError('No instance name is specified.')
    vol_size = int(kargs.get('vol_size', '100'))
    vol_name = kargs.get('vol_name', instance_name + '_vol_test')
    vol_type = kargs.get('vol_type', 'gp2')
    if vol_type not in volume_type:
        raise ValueError('Invalid volume type, please check.')

    ec2_op = ec2_operations(config_file)
    if op == 'create':
        ec2_op.create_instance(ami, instance_name)
    elif op == 'start':
        ec2_op.start_instance(instance_name)
    elif op == 'stop':
        ec2_op.stop_instance(instance_name)
    elif op == 'terminate':
        ec2_op.terminate_instance(instance_name, False)
    elif op == 'delete':
        ec2_op.terminate_instance(instance_name, True)
    elif op == 'add_volume':
        volume_id = ec2_op.create_volume(vol_name, vol_size, vol_type)
        ec2_op.attach_volume(instance_name, volume_id)
    elif op == 'detach_volume':
        volume_id = ec2_op.get_volume_id_from_tag(vol_name)
        ec2_op.delete_volume(volume_id)
    elif op == 'delete_volume':
        volume_id = ec2_op.get_volume_id_from_tag(vol_name)
        ec2_op.delete_volume(volume_id)
    elif op == 'state':
        state = ec2_op.get_instance_running_status(instance_name)
        print state
    elif op == 'ip':
        ip = ec2_op.get_instance_private_ip(instance_name)
        print ip
    else:
        raise ValueError('No such operation provided')

if __name__ == '__main__':
    kargs = check_params()
    start(kargs)
