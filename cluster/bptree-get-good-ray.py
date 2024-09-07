import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "fix_path", help="path to .fix repository", type=str)
parser.add_argument( "key_list", help="path to list of keys", type=str)
parser.add_argument( "num_of_keys", help="the number of keys to get", type=int)
parser.add_argument( "visited", help="list of keys that will be visited", type=str)
args = parser.parse_args()

key_list = []
with open( args.key_list, 'r' ) as f:
    for i in range( 0, args.num_of_keys ):
        key_list.append( int( f.readline().rstrip() ) )
fix_path = args.fix_path

visited_list = set()
with open( args.visited, 'r' ) as f:
    for key in f.read().splitlines():
        visited_list.add( key )

import ray
ray.init()

def decode( handle ):
    return base64.b16decode( handle.upper() )

def encode( handle ):
    return base64.b16encode( handle ).decode("utf-8").lower()

@ray.remote
class Loader:
    def __init__( self ):
        # prefix_map maps from handle[:48] to handle[48:]
        self.prefix_map = {}
        for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
            self.prefix_map[filename[:48]] = filename[48:]

    def get_object( self, handle ):
        prefix = encode( handle )
        filename = prefix + self.prefix_map[prefix]

        with open( os.path.join( fix_path, "data/", filename ), 'rb') as file:
            data = file.read()
        return data

    # Return list of prefixes that are at this loader
    def keys( self ):
        return self.prefix_map.keys()

loaders = []
loader_index = 0;
key_to_loader_map = {}
for node in nodes:
    loader = Loader.options(resources={ node : 0.0001 }).remote()
    keys = ray.get( loader.keys.remote() )
    for key in keys:
        if key in visited_list:
            raw = decode( key )
            key_to_loader_map[raw[:4]] = loader_index
    loaders.append( loader )
    loader_index += 1

def get_object( raw, key_to_loader_map ):
    if raw[30] | 0b11111000 == 0b11111000:
        local_loader_index = nodes.index( "node:" + ray._private.services.get_node_ip_address() )
        return loaders[local_loader_index].remote( raw )
    else:
        loader_index = key_to_loader_map[raw[:4]]
        return loaders[loader_index].remote( raw )

def get_entry( data, i ):
    return data[ int(i) * 32: int( i + 1 ) *32 ] )

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) / 4 ) ):
        x = int.from_bytes( keys[ int(i * 4):int(( i + 1 ) * 4) ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) / 4

@ray.remote
def bptree_get_good_style( is_odd, curr_level_data, keys_data, key ):
    if is_odd:
        return bptree_get_good_style.remote( False, curr_level_data, get_object( get_entry( curr_level_data, 0 ), key_to_loader_map ), key )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                return get_object( get_entry( curr_level_data, idx ), key_to_loader_map )
            else:
                return "Not found"
        else:
            return bptree_get_good_style.remote( True, get_object( get_entry( curr_level_data, idx + 1 ), key_to_loader_map ), "", key )

@ray.remote
def bptree_get_good_style_collect( bptree_root, key ):
    ref = bptree_get_good_style.remote( True, get_object( bptree_root, key_to_loader_map ), "", key )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ref

bptree_root = decode( os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) ) 

refs = []
for key in key_list:
    refs.append( bptree_get_good_style_collect.remote( bptree_root, key ) )

ray.get( refs )
