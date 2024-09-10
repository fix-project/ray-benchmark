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
parser.add_argument( "output_bucket", help="Input file name", type=str )
parser.add_argument( "minio_port", help="port to minio client", type=int)
args = parser.parse_args()

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

import ray
ray.init()

@ray.remote
def ray_subprocess( binary_ref, input_json_dump ):
    binary = ray.get( binary_ref[0] )
    local_executable_path = os.path.join( "/tmp", str( ray.get_runtime_context().get_task_id() ) + "-binary" )
    with open( local_executable_path, 'wb' ) as file:
        file.write( binary )
    subprocess.check_call(['chmod', '+x', local_executable_path])
    child = subprocess.Popen( [local_executable_path, input_json_dump], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = child.communicate()
    return json.loads( out.splitlines()[-1] ) 


start = time.monotonic()

with open( os.path.join( args.program_path, "wasm-to-c-minio" ), 'rb' ) as file:
    wasm_to_c_binary = file.read()
wasm_to_c_binary_ref = ray.put( wasm_to_c_binary )
wasm_to_c_input = {
        "input_bucket": args.input_bucket,
        "input_file": args.input_file,
        "output_bucket": args.output_bucket,
        "minio_url" : "localhost:" + str( args.minio_port ),
        }

wasm_to_c_output = ray.get( ray_subprocess.remote( [wasm_to_c_binary_ref], json.dumps( wasm_to_c_input ) ) )

with open( os.path.join( args.program_path, "c-to-elf-minio" ), 'rb' ) as file:
    c_to_elf_binary = file.read()
c_to_elf_binary_ref = ray.put( c_to_elf_binary )

refs = []
for i in range( 0, wasm_to_c_output["output_number"] ):
    c_to_elf_input = {
            "bucket": args.output_bucket,
            "index" : i,
            "minio_url" : "localhost:" + str( args.minio_port ),
            }
    ref.append( ray_subprocess.remote( [c_to_elf_binary_ref], json.dumps( c_to_elf_input ) ) )
c_to_elf_outputs = ray.get( refs )

with open( os.path.join( args.program_path, "link-elfs-minio" ), 'rb' ) as file:
    link_elfs_binary = file.read()
link_elfs_binary_ref = ray.put( link_elfs_binary )

link_elfs_input = {
        "bucket" : args.output_bucket,
        "last_index" : wasm_to_c_output["output_number"] - 1,
        "output_name" : "out-" + args.input_file,
        "minio_url" : "localhost:" + str( args.minio_port ),
        }
ray.get( ray_subprocess.remote( [link_elfs_binary_ref], json.dumps( link_elfs_input ) ) )

get_object_from_minio( args.output_bucket, "out-" + args.input_file )
end = time.monotonic()

print ( end - start )
