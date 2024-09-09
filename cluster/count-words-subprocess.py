import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument("program_path", help="", type=str)
parser.add_argument("needle", help="needle", type=str)
parser.add_argument("minio_port", help="port to minio client", type=int)
parser.add_argument("input_bucket", help="name of input file bucket", type=str)
parser.add_argument("num_chunk", help="number of input chunks", type=int)
args = parser.parse_args()

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

import ray
ray.init()

def mapper( mapper_ref, needle, index ):
    input = {
            "input_bucket" : args.input_bucket,
            "input_file" : "chunk" + str( index ),
            "minio_url" : "localhost:" + str( args.minio_port ),
            "query": needle
            }
    return ray_subprocess( mapper_ref, json.dumps( input ) )

def reducer( reducer_ref, x, y ):
    input = {
            "input_x": x,
            "input_y": y,
            }
    return ray_subprocess( reducer_ref, json.dumps( input ) )

@ray.remote
def mapreduce( mapper_ref, reducer_ref, needle, start, end ):
    if ( start == end or start == end - 1 ):
        return mapper( mapper_ref, needle, start )
    else:
        split = start + ( end - start ) / 2
        first = mapreduce.remote( needle, chunk_list, start, split )
        second = mapreduce.remote( handle, chunk_list, split, end )
        x = ray.get( first )
        y = ray.get( second )
        return reducer( x, y )

start = time.monotonic()

with open( os.path.join( args.program_path, "count-words-minio" ), 'rb' ) as file:
    count_words_binary = file.read()
count_words_binary_ref = ray.put( bptree_get_binary )

with open( os.path.join( args.program_path, "merge-counts-minio" ), 'rb' ) as file:
    merge_counts_binary = file.read()
merge_count_binary_ref = ray.put( bptree_get_binary )

ray.get( mapreduce.remote( [count_words_binary_ref], [merge_count_binary_ref], args.needle, 0, num_chunk ) )

print( end - start )
    

