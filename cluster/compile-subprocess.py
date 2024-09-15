import argparse
import os
import time 
import json
import subprocess
import boto3

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "program_path", help="", type=str)
parser.add_argument( "input_bucket", help="Input file bucket name", type=str)
parser.add_argument( "input_file", help="Input file name", type=str )
parser.add_argument( "output_bucket", help="output bucket name", type=str )
parser.add_argument( "minio_port", help="port to minio client", type=int)
args = parser.parse_args()

import ray
ray.init()

@ray.remote(runtime_env={"pip":["boto3"]})
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
def load_program( binary_ref, program_name ):
    binary = ray.get( ray.get( binary_ref[0] ) )
    binary_path = os.path.join( "/home/ubuntu", program_name )
    with open( binary_path, 'wb' ) as file:
        file.write( binary )
    subprocess.check_call(['chmod', '+x', binary_path])
    return binary_path

@ray.remote
def ray_subprocess( binary_path, input_json_dump ):
    child = subprocess.Popen( [binary_path, input_json_dump], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = child.communicate()
    return json.loads( out.splitlines()[-1] ) 

@ray.remote
def get_program( program_name ):
    with open( os.path.join( args.program_path, program_name ), 'rb' ) as file:
        binary = file.read()
    return ray.put( binary )

def load_program_to_every_node( binary_ref, program_name ):
    refs = []
    for node in nodes:
        refs.append( load_program.options(resources={ node: 0.0001 }).remote( [binary_ref], program_name ) )
    return ray.get( refs )[0]

@ray.remote
def do_compile():
    wasm_to_c_path = load_program_to_every_node( get_program.options(resources={ "node:172.31.8.132": 0.0001 }).remote("wasm-to-c-minio"), "wasm-to-c" )

    wasm_to_c_input = {
            "input_bucket": args.input_bucket,
            "input_file": args.input_file,
            "output_bucket": args.output_bucket,
            "minio_url" : "localhost:" + str( args.minio_port ),
            }

    wasm_to_c_output = ray.get( ray_subprocess.remote( wasm_to_c_path, json.dumps( wasm_to_c_input ) ) )

    c_to_elf_binary_path = load_program_to_every_node( get_program.options(resources={ "node:172.31.8.132": 0.0001 }).remote("c-to-elf-minio"), "c-to-elf" )

    refs = []
    for i in range( 0, wasm_to_c_output["output_number"] ):
        c_to_elf_input = {
                "bucket": args.output_bucket,
                "index" : i,
                "minio_url" : "localhost:" + str( args.minio_port ),
                }
        refs.append( ray_subprocess.remote( c_to_elf_binary_path, json.dumps( c_to_elf_input ) ) )
    c_to_elf_outputs = ray.get( refs )

    link_elfs_binary_path = load_program_to_every_node( get_program.options(resources={ "node:172.31.8.132": 0.0001 }).remote("link-elfs-minio"), "link-elfs" )

    link_elfs_input = {
            "bucket" : args.output_bucket,
            "last_index" : wasm_to_c_output["output_number"] - 1,
            "output_name" : "out-" + args.input_file,
            "minio_url" : "localhost:" + str( args.minio_port ),
            }
    ray.get( ray_subprocess.remote( link_elfs_binary_path, json.dumps( link_elfs_input ) ) )

    return ray.get( get_object_from_minio.remote( args.output_bucket, "out-" + args.input_file ) )

start = time.monotonic()
ray.get( do_compile.remote() )
end = time.monotonic()

print ( end - start )
