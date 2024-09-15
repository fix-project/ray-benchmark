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


def mapper( mapper_path, needle, index ):
    input = {
            "input_bucket" : args.input_bucket,
            "input_file" : "chunk" + str( index ),
            "minio_url" : "localhost:" + str( args.minio_port ),
            "query": needle
            }
    return ray.get( ray_subprocess.remote( mapper_path, json.dumps( input ) ) )

def reducer( reducer_path, x, y ):
    input = {
            "input_x": x,
            "input_y": y,
            }
    return ray.get( ray_subprocess.remote( reducer_path, json.dumps( input ) ) )

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
def do_countwords():
    count_words_path = load_program_to_every_node( get_program.options(resources={ "node:172.31.8.132": 0.0001 }).remote("count-words-minio"), "count-words" )
    merge_counts_path = load_program_to_every_node( get_program.options(resources={ "node:172.31.8.132": 0.0001 }).remote("merge-counts-minio"), "merge-counts" )

    return ray.get( mapreduce.remote( count_words_path, merge_counts_path, args.needle, 0, args.num_chunk ) )

start = time.monotonic()
ray.get( do_countwords.remote() ) 
end = time.monotonic()

print( end - start )
