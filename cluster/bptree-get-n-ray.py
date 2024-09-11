import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument("style", help="bptree style", type=str)
parser.add_argument("fix_path", help="path to .fix repository", type=str)
parser.add_argument("key_list", help="path to list of keys", type=str)
parser.add_argument("num_of_keys", help="the number of keys to get", type=int)
parser.add_argument("visited", help="list of keys that will be visited", type=str)
parser.add_argument( "n", help="number of leaf nodes to get", type=int)
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
        # prefix_map maps from handle[:48] to handle[48:]
        self.prefix_map = {}
        for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
            self.prefix_map[filename[:48]] = filename[48:]

    def get_object( self, handle ):
        if handle[30] | 0b11111000 == 0b11111000:
            size = handle[30] >> 3
            return handle[:size] 

        prefix = encode( handle )[:48]

        if prefix == "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7":
            return ""

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
        size = raw[30] >> 3
        return ray.put( raw[:size] )
    elif raw[:4] in key_to_loader_map:
        loader_index = key_to_loader_map[raw[:4]]
        return loaders[loader_index].get_object.remote( raw )
    else:
        return ray.put( "" )

def get_object_deref( raw, key_to_loader_map ):
    result = get_object( raw, key_to_loader_map )
    if ( isinstance( result, ray._raylet.ObjectRef ) ):
        result = ray.get( result )
    return result

def get_entry( data, i ):
    return data[ int(i) * 32: int( i + 1 ) *32 ]

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) // 4 ) ):
        x = int.from_bytes( keys[ int(i * 4):int(( i + 1 ) * 4) ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) // 4

@ray.remote
def bptree_get_leaf_nodes( acc, curr_level_data, n, key_to_loader_map ):
    if len( curr_level_data ) == 0:
        return acc

    leaf_node = []
    for i in range( 1, int( len( curr_level_data ) // 32 ) - 1 ):
        leaf_node.append( get_object( get_entry( curr_level_data, i ), key_to_loader_map ) )
    acc.append( leaf_node )

    if len( acc ) >= n:
        data = []
        for node in acc:
            data.append( ray.get( node ) )
        return data
    else:
        return bptree_get_leaf_nodes.remote( acc, get_object( get_entry( curr_level_data, int( len( curr_level_data ) // 32 ) - 1 ), key_to_loader_map ), n, key_to_loader_map )

@ray.remote
def bptree_get_n_good_style( is_odd, curr_level_data, keys_data, key, n ):
    if is_odd:
        return bptree_get_n_good_style.remote( False, curr_level_data, get_object( get_entry( curr_level_data, 0 ), key_to_loader_map ), key, n )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                leaf_nodes = []
                leaf_node = []

                for i in range( idx, int( len( curr_level_data ) // 32 ) - 1 ):
                    leaf_node.append( get_object( get_entry( curr_level_data, i ), key_to_loader_map ) )
                leaf_nodes.append( leaf_node )

                if n > 1:
                    return bptree_get_leaf_nodes.remote( leaf_nodes, get_object( get_entry( curr_level_data, int( len( curr_level_data ) // 32 ) - 1 ), key_to_loader_map ), n, key_to_loader_map )
            else:
                return "Not found"
        else:
            return bptree_get_n_good_style.remote( True, get_object( get_entry( curr_level_data, idx + 1 ), key_to_loader_map ), "", key, n )

@ray.remote
def bptree_get_n_good_style_collect( bptree_root, key ):
    ref = bptree_get_n_good_style.remote( True, get_object( bptree_root, key_to_loader_map ), "", key, args.n )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )

    return ref 

@ray.remote
def bptree_get_n_bad_style( root, key ):
    curr_level = root

    while True:
        data = get_object_deref( curr_level, key_to_loader_map )
        keys = get_object_deref( get_entry( data, 0 ), key_to_loader_map )
        isleaf = keys[0] == 1
        keys = keys[1:]
        idx = upper_bound( keys, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                leaf_nodes = []
                leaf_node = []

                for i in range( idx, int( len( data ) // 32 ) - 1 ):
                    leaf_node.append( get_object( get_entry( data, i ), key_to_loader_map ) )
                leaf_nodes.append( leaf_node )
                
                for i in range( 1, args.n ):
                    leaf_node = []
                    data = get_object_deref( get_entry( data, int( len( data ) // 32 ) - 1 ), key_to_loader_map )
                    for j in range( 1, int( len( data ) // 32 ) - 1 ):
                        leaf_node.append( get_object( get_entry( data, j ), key_to_loader_map ) )
                    leaf_nodes.append( leaf_node )

                leaf_data = []
                for i in range( 0, args.n ):
                    leaf_data.append( ray.get( leaf_nodes[i] ) )

                return leaf_data
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

bptree_root = decode( os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) ) 

start = time.monotonic()
refs = []
if ( args.style == "good" ):
    for key in key_list:
        refs.append( bptree_get_n_good_style_collect.remote( bptree_root, key ) )
else:
    for key in key_list:
        refs.append( bptree_get_n_bad_style.remote( bptree_root, key ) )
ray.get( refs )
end = time.monotonic()
print( end - start )
