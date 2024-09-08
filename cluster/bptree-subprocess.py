import argparse
import os
import time 
import json
import subprocess

parser = argparse.ArgumentParser("bptree-get-ray")
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

@ray.remote
def ray_subprocess( binary, input_json_dump ):
    local_executable_path = os.path.join( "/tmp", str( ray.get_runtime_context().get_task_id() ) + "-binary" )
    with open( local_executable_path, 'wb' ) as file:
        file.write( binary )
    subprocess.check_call(['chmod', '+x', local_executable_path])
    child = subprocess.Popen( [local_executable_path, input_json_dump], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = child.communicate()
    return json.loads( out.splitlines()[-1] ) 

start = time.time()

with open( os.path.join( program_path, "bptree-get-minio" ), 'rb' ) as file:
    bptree_get_binary = file.read()
bptree_get_binary_ref = ray.put( bptree_get_binary )

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
    refs.append( ray_subprocess.remote( bptree_get_binary_ref, json.dumps( input ) ) )

ray.get( refs )
end = time.time()

print ( end - start )
