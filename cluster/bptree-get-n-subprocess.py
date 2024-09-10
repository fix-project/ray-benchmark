import argparse
import os
import time 
import json
import subprocess

parser = argparse.ArgumentParser("bptree-get-n-ray")
parser.add_argument( "program_path", help="", type=str)
parser.add_argument( "key_list", help="path to list of keys", type=str)
parser.add_argument( "num_of_keys", help="the number of keys to get", type=int)
parser.add_argument( "minio_port", help="port to minio client", type=int)
parser.add_argument( "n", help="number of leaf nodes to get", type=int)
args = parser.parse_args()

key_list = []
with open( args.key_list, 'r' ) as f:
    for i in range( 0, args.num_of_keys ):
        key_list.append( int( f.readline().rstrip() ) )

import ray
ray.init()

@ray.remote
def get_object_from_minio( bucket, name ):
    s3_target = boto3.resource('s3', 
                               endpoint_url='http://localhost:' + str( args.minio_port ) ,
                               aws_access_key_id='minioadmin',
                               aws_secret_access_key='minioadmin',
                               aws_session_token=None,
                               config=boto3.session.Config(signature_version='s3v4'),
                               verify=False )

    obj = s3_target.Object( bucket, name )
    return obj.get()['Body'].read()

nodes = []
for key in ray.cluster_resources().keys():
    if key.startswith( "node:" ) and not( key == "node:__internal_head__" ):
        nodes.append( key )
        print( key )

@ray.remote
def load_program( binary_ref ):
    binary = ray.get( binary_ref[0] )
    with open( "/home/ubuntu/bptree-get-n", 'wb' ) as file:
        file.write( binary )
    subprocess.check_call(['chmod', '+x', "/home/ubuntu/bptree-get-n"])

@ray.remote
def ray_subprocess( input_json_dump ):
    local_executable_path = "/home/ubuntu/bptree-get-n" 
    child = subprocess.Popen( [local_executable_path, input_json_dump], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = child.communicate()
    return json.loads( out.splitlines()[-1] ) 

start = time.monotonic()

with open( os.path.join( args.program_path, "bptree-get-n-minio" ), 'rb' ) as file:
    bptree_get_binary = file.read()
bptree_get_binary_ref = ray.put( bptree_get_binary )

refs = []
for node in nodes:
    refs.append( load_program.options(resources={ node: 0.0001 }).remote( [bptree_get_binary_ref] ) )
ray.get( refs )

refs = []
for key in key_list:
    input = {
            "input_bucket" : "bptree4",
            "tree_root" : "b157cae256abc75fdd1f1870177dba2fb95afdab0f0236180400000000000300",
            "minio_url" : "localhost:" + str( args.minio_port ),
            "key" : key,
            "output_bucket" : "bptree-n-out",
            "output_file" : "out-" + str(key),
            "n" : args.n
            }
    refs.append( ray_subprocess.remote( json.dumps( input ) ) )

ray.get( refs )

refs = []
for key in key_list:
    refs.append( get_object_from_minio.remote( "bptree-n-out", "out-" + str( key ) )
ray.get( refs )

end = time.monotonic()
print ( end - start )
