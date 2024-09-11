import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument("style", help="bptree style", type=str)
parser.add_argument("fix_path", help="path to .fix repository", type=str)
parser.add_argument("key_list", help="path to list of keys", type=str)
parser.add_argument("num_of_keys", help="the number of keys to get", type=int)
parser.add_argument( "n", help="number of leaf nodes to get", type=int)
args = parser.parse_args()

key_list = []
with open( args.key_list, 'r' ) as f:
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
        # prefix_map maps from handle[:48] to handle[48:]
        self.prefix_map = {}
        for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
            self.prefix_map[filename[:48]] = filename[48:]
        self.empty_tree = decode( "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7" )
        self.empty_ref = ray.put( "" )
        self.refs = []

    def to_handle( self, handle ):
        if handle[30] | 0b11111000 == 0b11111000:
            size = handle[30] >> 3
            ref = ray.put( handle[:size] )
            self.refs.append( ref )
            return ref

        if handle[:24] == self.empty_tree:
            return self.empty_ref

        return ( handle, self.key_to_loader_map.get( handle[:8], -1 ) )

    def get_object( self, handle ):
        prefix = encode( handle )[:48]
        filename = prefix + self.prefix_map[prefix]

        with open( os.path.join( fix_path, "data/", filename ), 'rb') as file:
            data = file.read()

        handles = []
        for i in range( 0, len( data ) // 32 ):
            handles.append( self.to_handle( data[ int(i) * 32: int( i + 1 ) *32 ] ) )
        
        return handles

    def set_map( self, key_to_loader_map ):
        self.key_to_loader_map = key_to_loader_map

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
        raw = decode( key )
        key_to_loader_map[raw[:8]] = loader_index
    loaders.append( loader )
    loader_index += 1

refs = []
for loader in loaders:
    refs.append( loader.set_map.remote( key_to_loader_map ) )
ray.get( refs )

def get_object( raw ):
    if ( isinstance( raw, ray._raylet.ObjectRef ) ):
        return raw
    else:
        loader_index = raw[1] 
        return loaders[raw[1]].get_object.remote( raw[0] )

def get_object_deref( raw ):
    return ray.get( get_object( raw ) )

def get_entry( data, i ):
    return data[ int( i ) ]

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) // 4 ) ):
        x = int.from_bytes( keys[ int(i * 4):int(( i + 1 ) * 4) ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) // 4

@ray.remote
def bptree_get_leaf_nodes( gotten, acc, curr_level_data, n ):
    if len( curr_level_data ) == 0:
        return acc

    gotten += 1

    for i in range( 1, len( curr_level_data ) - 1 ):
        acc.append( get_object( get_entry( curr_level_data, i ) ) )

    if gotten >= n:
        return acc
    else:
        return bptree_get_leaf_nodes.remote( gotten, acc, get_object( get_entry( curr_level_data, len( curr_level_data ) - 1 ) ), n )

@ray.remote
def bptree_get_n_good_style( is_odd, curr_level_data, keys_data, key, n ):
    if is_odd:
        return bptree_get_n_good_style.remote( False, curr_level_data, get_object( get_entry( curr_level_data, 0 ) ), key, n )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                leaf_nodes = []

                for i in range( idx, len( curr_level_data ) - 1 ):
                    leaf_nodes.append( get_object( get_entry( curr_level_data, i ) ) )

                if n > 1:
                    return bptree_get_leaf_nodes.remote( 1, leaf_nodes, get_object( get_entry( curr_level_data, len( curr_level_data ) - 1 ) ), n )
                else:
                    return leaf_nodes
            else:
                return "Not found"
        else:
            return bptree_get_n_good_style.remote( True, get_object( get_entry( curr_level_data, idx + 1 ) ), "", key, n )

@ray.remote
def bptree_get_n_good_style_collect( bptree_root, key ):
    ref = bptree_get_n_good_style.remote( True, get_object( bptree_root ), "", key, args.n )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ray.get( ref )

@ray.remote
def bptree_get_n_bad_style( root, key ):
    curr_level = root

    while True:
        data = get_object_deref( curr_level )
        keys = get_object_deref( get_entry( data, 0 ) )
        isleaf = keys[0] == 1
        keys = keys[1:]
        idx = upper_bound( keys, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                acc = []

                for i in range( idx, len( data ) - 1 ):
                    acc.append( get_object( get_entry( data, i ) ) )
                
                for i in range( 1, args.n ):
                    data = get_object_deref( get_entry( data, len( data ) - 1 ) )
                    for j in range( 1, len( data ) - 1 ):
                        acc.append( get_object( get_entry( data, j ) ) )

                return ray.get( acc )
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

bptree_root_raw = decode( os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) ) 
bptree_root = ( bptree_root_raw, key_to_loader_map[bptree_root_raw[:8]] )

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
