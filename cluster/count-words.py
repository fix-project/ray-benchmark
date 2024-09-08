import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument("style", help="bptree style", type=str)
parser.add_argument("fix_path", help="path to .fix repository", type=str)
parser.add_argument("chunks", help="path to list of chunks", type=str)
parser.add_argument("needle", help="needle", type=str)
args = parser.parse_args()

chunk_list = []
with open( args.chunks, 'r' ) as f:
    for i in range( 0, args.num_of_keys ):
        key_list.append( int( f.readline().rstrip() ) )

fix_path = args.fix_path

import ray
ray.init()

nodes = []
for key in ray.cluster_resources().keys():
    if key.startswith( "node:" ) and not( key == "node:__internal_head__" ):
        nodes.append( key )
        print( key )

def decode( handle ):
    return base64.b16decode( handle.upper() )

def encode( handle ):
    return base64.b16encode( handle ).decode("utf-8").lower()

@ray.remote
class Loader:
    def __init__( self ):
        return

    def get_object( self, handle ):
        filename = encode( handle )

        with open( os.path.join( fix_path, "data/", filename ), 'rb') as file:
            data = file.read()
        return data

    # Return list of prefixes that are at this loader
    def keys( self ):
        key_list = []
        for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
            key_list.append( filename )
        return key_list

loaders = []
loader_index = 0;
key_to_loader_map = {}
for node in nodes:
    loader = Loader.options(resources={ node : 0.0001 }).remote()
    keys = ray.get( loader.keys.remote() )
    for key in keys:
        raw = decode( key )
        key_to_loader_map[raw[:4]] = loader_index
    loaders.append( loader )
    loader_index += 1

def get_object( raw, key_to_loader_map ):
    if raw[:4] in key_to_loader_map:
        loader_index = key_to_loader_map[raw[:4]]
        return loaders[loader_index].get_object.remote( raw )
    else:
        local_loader_index = nodes.index( "node:" + ray._private.services.get_node_ip_address() )
        return loaders[local_loader_index].get_object.remote( raw )

@ray.remote
def count_words(needle: bytes, haystack: bytes) -> int:
    count = 0
    if len(needle) <= len(haystack):
        for i in range(0, len(haystack) - len(needle) + 1):
            if needle == haystack[i:i+len(needle)]:
                count += 1
    return count

def merge_counts(x, y):
    return x + y

def mapper_good_style( needle, handle ):
    return count_words.remote( needle, get_object( handle, key_to_loader_map ) )

@ray.remote
def reducer_good_style( x, y ):
    if ( isinstance( x, ray._raylet.ObjectRef ) or isinstance( y, ray._raylet.ObjectRef ) ):
        return reducer_good_style.remote( x, y )
    else:
        return merge_counts( x, y )

def mapper_bad_style( needle, handle ):
    chunk = ray.get( get_object( handle, key_to_loader_map ) )
    return count_words( needle, chunk )

def reducer_bad_style( x, y ):
    return merge_counts( left, right )

@ray.remote
def mapreduce_good_style( needle, chunk_list, start, end ):
    if ( start == end or start == end - 1 ):
        return mapper_good_style( needle, chunk_list[start] )
    else:
        split = start + ( end - start ) / 2
        first = mapreduce_good_style.remote( needle, chunk_list, start, split )
        second = mapreduce_good_style.remote( needle, chunk_list, split, end )
        return reducer_good_style.remote( first, second )

@ray.remote
def mapreduce_good_style_collect( needle, chunk_list ):
    ref = mapreduce_good_style.remote( needle, chunk_list, 0, len( chunk_list ) )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ref

@ray.remote
def mapreduce_bad_style( needle, chunk_list, start, end ):
    if ( start == end or start == end - 1 ):
        return mapper_bad_style( needle, chunk_list[start] )
    else:
        split = start + ( end - start ) / 2
        first = mapreduce_bad_style.remote( needle, chunk_list, start, split )
        second = mapreduce_bad_style.remote( handle, chunk_list, start, split )
        x = ray.get( first )
        y = ray.get( second )
        return reducer_bad_style( first, second )


start = time.monotonic()
if ( args.style == "good" ):
    ray.get( mapreduce_good_style_collect.remote( args.needle, chunk_list ) )
else:
    ray.get( mapreduce_bad_style.remote( args.needle, chunk_list, 0, len( chunk_list ) ) )
end = time.monotonic()
print( end - start )
    

