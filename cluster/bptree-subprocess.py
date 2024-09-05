import argparse
import os
import time 
import boto3
import json
import subprocess

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "program_bucket", help="minio bucket of programs", type=str)
parser.add_argument( "program_path", help="", type=str)
parser.add_argument( "key_list", help="path to list of keys", type=str)
parser.add_argument( "num_of_keys", help="the number of keys to get", type=int)
parser.add_argument( "minio_port", help="port to minio client", type=int)
args = parser.parse_args()

key_list = []
with open( args.key_list, 'r' ) as f:
    for i in range( 0, args.num_of_keys ):
        key_list.append( int( f.readline().rstrip() ) )

import ray
ray.init()

nodes = []
for key in ray.cluster_resources().keys():
    if key.startswith( "node:" ) and not( key == "node:__internal_head__" ):
        nodes.append( key )
        print( key )

@ray.remote
def get_program( program_bucket, program_name, program_path ):
    s3_target = boto3.resource('s3', 
                               endpoint_url='http://localhost:' + str( args.minio_port ) ,
                               aws_access_key_id='minioadmin',
                               aws_secret_access_key='minioadmin',
                               aws_session_token=None,
                               config=boto3.session.Config(signature_version='s3v4'),
                               verify=False )
    obj = s3_target.Object( program_bucket, program_name )
    with open( program_path, 'wb' ) as file:
        file.write( obj.get()['Body'].read() )
    subprocess.check_call(['chmod', '+x', args.program_path])

@ray.remote
def bptree_subprocess( input_json_dump ):
    #get_program( args.program_bucket, "bptree-get", args.program_path )
    child = subprocess.Popen( [args.program_path, input_json_dump], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = child.communicate()
    return child.returncode

start = time.time()
refs = []
for node in nodes:
    refs.append( get_program.options(resources={ node: 0.0001 }).remote( args.program_bucket, "bptree-get", args.program_path ) )
ray.get( refs )

refs = []
for key in key_list:
    input = {
            "input_bucket" : "bptree4",
            "tree_root" : "b157cae256abc75fdd1f1870177dba2fb95afdab0f0236180400000000000300",
            "minio_url" : "localhost:" + str( args.minio_port ),
            "key" : key,
            "output_bucket" : "bptree-out",
            "output_file" : "out-" + str(key)
            }

    refs.append( bptree_subprocess.remote( json.dumps( input ) ) )

ray.get( refs )
end = time.time()

print ( end - start )
