import argparse
import os
import time 
import boto3
import json
import subprocess

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "program_bucket", help="minio bucket of programs", type=str)
parser.add_argument( "program_path", help="", type=str)
parser.add_argument( "input_bucket", help="Input file bucket name", type=str)
parser.add_argument( "input_file", help="Input file name", type=str )
parser.add_argument( "output_bucket", help="Input file name", type=str )
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

@ray.remote
def get_program( program_bucket, program_name, program_path ):
    local_executable_path = os.path.join( program_path, program_name )
    with open( local_executable_path, 'wb' ) as file:
        file.write( get_object_from_minio( program_bucket, program_path ) )
    subprocess.check_call(['chmod', '+x', local_executable_path])
    return local_executable_path

@ray.remote
def ray_subprocess( executable_path, input_json_dump ):
    child = subprocess.Popen( [executable_path, input_json_dump], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = child.communicate()
    lastline = out.readlines()[-1]
    return json.loads( lastline ) 

def load_program_on_every_node( program_name ):
    refs = []
    for node in nodes:
        refs.append( get_program.options(resources={ node: 0.0001 }).remote( args.program_bucket, program_name, args.program_path ) )
    return ray.get( refs )[0]

start = time.time()

wasm_to_c_path = load_program_on_every_node( "wasm-to-c" ) 
wasm_to_c_input = {
        "input_bucket": args.input_bucket,
        "input_file": args.input_file
        "output_bucket": args.output_bucket
        }
wasm_to_c_output = ray.get( ray_subprocess.remote( wasm_to_c_path, json.dumps( wasm_to_c_input ) ) )

c_to_elf_path = load_program_on_every_node( "c-to-elf" )
refs = []
for i in range( 0, wasm_to_c_output["output_number"] ):
    c_to_elf_input = {
            "bucket": args.output_bucket,
            "index" : i
            }
    ref.append( ray_subprocess.remote( c_to_elf_path, json.dumps( c_to_elf_input) ) )
c_to_elf_outputs = ray.get( refs )

link_elfs_path = load_program_on_every_node( "link-elfs" )
link_elfs_input = {
        "bucket" : args.output_bucket
        "last_index" : wasm_to_c_output["output_number"] - 1
        "output_name" : "out-" + args.input_file
        }
ray.get( ray_subprocess.remote( link_elfs_path, json.dumps( link_elfs_input ) ) )

get_object_from_minio( args.output_bucket, "out-" + args.input_file )
end = time.time()

print ( end - start )
