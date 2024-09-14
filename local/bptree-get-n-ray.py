import ray
ray.init(num_cpus=1)

import base64
import argparse
import os
import time

parser = argparse.ArgumentParser("bptree-get-n-ray")
parser.add_argument("style", help="bptree style", type=str)
parser.add_argument("fix_path", help="path to .fix repository", type=str)
parser.add_argument("key_list", help="key to lookup", type=str)
parser.add_argument("begin_key_index", help="beginning index to look up in key_list", type=int)
parser.add_argument("num_of_keys", help="key to lookup", type=int)
parser.add_argument("n", help="number of keys to get", type=int)
args = parser.parse_args()

key_list = []
with open( args.key_list, 'r' ) as f:
    for i in range( 0, args.begin_key_index + args.num_of_keys ):
        if i < args.begin_key_index:
            f.readline()
        else:
            key_list.append( int( f.readline().rstrip() ) )

def decode( handle ):
    return base64.b16decode( handle.upper() )

def encode( handle ):
    return base64.b16encode( handle ).decode("utf-8").lower()

@ray.remote
class Loader:
    def __init__( self ):
        self.prefix_map = {}
        for filename in os.listdir( os.path.join( args.fix_path, "data/" ) ):
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

        return handle

    def get_object( self, handle ):
        prefix = encode( handle )[:48]
        filename = prefix + self.prefix_map[prefix]

        with open( os.path.join( args.fix_path, "data/", filename ), 'rb') as file:
            data = file.read()

        handles = []
        for i in range( 0, len( data ) // 32 ):
            handles.append( self.to_handle( data[ int(i) * 32: int( i + 1 ) *32 ] ) )
        return handles

# Create an actor
loader = Loader.remote()

def get_object( raw ):
    if ( isinstance( raw, ray._raylet.ObjectRef ) ):
        return raw
    else:
        return loader.get_object.remote( raw )

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
def filter( leaf_nodes, n ):
    leaf_nodes_data = ray.get( leaf_nodes )
    keys = []
    for d in leaf_nodes_data:
        if len( d ) > 0:
            keys.append( d )
            if len( keys ) == n:
                return keys

@ray.remote
def bptree_get_leaf_nodes( gotten, acc, curr_level_data, estimate_num_nodes, n ):
    if len( curr_level_data ) == 0:
        return acc

    gotten += 1
    for i in range( 1, len( curr_level_data ) - 1 ):
        acc.append( get_object( get_entry( curr_level_data, i ) ) )

    if gotten >= estimate_num_nodes:
        return filter.remote( acc, n )
    else:
        return bptree_get_leaf_nodes.remote( gotten, acc, get_object( get_entry( curr_level_data, len( curr_level_data ) - 1 ) ), estimate_num_nodes, n )

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

                if ( n == 1 ):
                    return [ray.get( get_object( get_entry( curr_level_data, idx ) ) )]

                leaf_node_size = len( curr_level_data )
                estimate_num_nodes = n // ( ( leaf_node_size - 2 ) // 2 )

                leaf_nodes = []
                for i in range( idx, len( curr_level_data ) - 1 ):
                    leaf_nodes.append( get_object( get_entry( curr_level_data, i ) ) )

                if estimate_num_nodes > 1:
                    return bptree_get_leaf_nodes.remote( 1, leaf_nodes, get_object( get_entry( curr_level_data, len( curr_level_data ) - 1 ) ), estimate_num_nodes, n )
                else:
                    return filter.remote( leaf_nodes, n )
            else:
                return "Not found"
        else:
            return bptree_get_n_good_style.remote( True, get_object( get_entry( curr_level_data, idx + 1 ) ), "", key, n )

@ray.remote
def bptree_get_n_good_style_collect( bptree_root, key ):
    ref = bptree_get_n_good_style.remote( True, get_object( bptree_root ), "", key, args.n )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ref

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
                if ( args.n == 1 ):
                    return [get_object_deref( get_entry( data, idx ) )]

                leaf_node_size = len( data )
                estimate_num_nodes = args.n // ( ( leaf_node_size - 2 ) // 2 )

                acc = []
                for i in range( idx, len( data ) - 1 ):
                    acc.append( get_object( get_entry( data, i ) ) )
                
                for i in range( 1, estimate_num_nodes ):
                    data = get_object_deref( get_entry( data, len( data ) - 1 ) )
                    for j in range( 1, len( data ) - 1 ):
                        acc.append( get_object( get_entry( data, j ) ) )

                return ray.get( filter.remote( acc, args.n ) )
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

bptree_root = decode( os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) )

start = time.monotonic()
for key in key_list:
    if ( args.style == "good" ):
        ray.get( bptree_get_n_good_style_collect.remote( bptree_root, key ) )
    else:
        ray.get( bptree_get_n_bad_style.remote( bptree_root, key ) )
end = time.monotonic()

print( end - start )
