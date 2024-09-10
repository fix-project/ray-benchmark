import base64
import argparse
import os
import time 
import subprocess
import json

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument("program_path", help="", type=str)
parser.add_argument("needle", help="needle", type=str)
parser.add_argument("minio_port", help="port to minio client", type=int)
parser.add_argument("input_bucket", help="name of input file bucket", type=str)
parser.add_argument("num_chunk", help="number of input chunks", type=int)
args = parser.parse_args()

import ray
ray.init()

nodes = []
for key in ray.cluster_resources().keys():
    if key.startswith( "node:" ) and not( key == "node:__internal_head__" ):
        nodes.append( key )
        print( key )

@ray.remote
def load_program( binary_ref, program_name ):
    binary = ray.get( binary_ref[0] )
    binary_path = os.path.join( "/home/ubuntu", program_name )
    with open( binary_path, 'wb' ) as file:
        file.write( binary )
    subprocess.check_call(['chmod', '+x', binary_path])
    return binary_path

def ray_subprocess( binary_path, input_json_dump ):
    child = subprocess.Popen( [binary_path, input_json_dump], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = child.communicate()
    return json.loads( out.splitlines()[-1] ) 


def mapper( mapper_path, needle, index ):
    input = {
            "input_bucket" : args.input_bucket,
            "input_file" : "chunk" + str( index ),
            "minio_url" : "localhost:" + str( args.minio_port ),
            "query": needle
            }
    return ray_subprocess( mapper_path, json.dumps( input ) )

def reducer( reducer_path, x, y ):
    input = {
            "input_x": x,
            "input_y": y,
            }
    return ray_subprocess( reducer_path, json.dumps( input ) )

@ray.remote
def mapreduce( mapper_path, reducer_path, needle, start: int, end: int ):
    if ( start == end or start == end - 1 ):
        return mapper( mapper_path, needle, start )
    else:
        split = start + ( end - start ) // 2
        first = mapreduce.remote( mapper_path, reducer_path, needle, start, split )
        second = mapreduce.remote( mapper_path, reducer_path, needle, split, end )
        x = ray.get( first )
        y = ray.get( second )
        return reducer( reducer_path, x, y )

start = time.monotonic()

with open( os.path.join( args.program_path, "count-words-minio" ), 'rb' ) as file:
    count_words_binary = file.read()
count_words_binary_ref = ray.put( count_words_binary )

refs = []
for node in nodes:
    refs.append( load_program.options(resources={ node: 0.0001 }).remote( [count_words_binary_ref], "count-words" ) )
count_words_path = ray.get( refs )[0]

with open( os.path.join( args.program_path, "merge-counts-minio" ), 'rb' ) as file:
    merge_counts_binary = file.read()
merge_count_binary_ref = ray.put( merge_counts_binary )

refs = []
for node in nodes:
    refs.append( load_program.options(resources={ node: 0.0001 }).remote( [merge_count_binary_ref], "merge-counts" ) )
merge_counts_path = ray.get( refs )[0]

print( ray.get( mapreduce.remote( count_words_path, merge_counts_path, args.needle, 0, args.num_chunk ) ) )

end = time.monotonic()

print( end - start )
