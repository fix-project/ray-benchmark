import ray
ray.init()

import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-n-ray")
parser.add_argument( "fix_path", help="path to .fix repository", type=str)
parser.add_argument( "n", help="number of leaf nodes to get", type=int)
args = parser.parse_args()
fix_path = args.fix_path
n = args.n

@ray.remote
class Loader:
    def __init__( self ):
        self.prefix_map = {}
        self.visited = set()
        for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
            self.prefix_map[filename[:48]] = filename[48:]

    def get_object( self, handle ):
        raw = base64.b16decode( handle.upper() )
        if raw[30] | 0b11111000 == 0b11111000:
            size = raw[30] >> 3
            return raw[:size] 

        prefix = handle[:48]

        if prefix == "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7":
            return ""

        self.visited.add( prefix )

        filename = prefix + self.prefix_map[prefix]

        with open( os.path.join( fix_path, "data/", filename ), 'rb') as file:
            data = file.read()
            return data

    def get_visited( self ):
        return self.visited

# Create an actor
loader = Loader.remote()

def get_entry( data, i ):
    return base64.b16encode( data[ int(i) * 32: int( i + 1 ) *32 ] ).decode("utf-8").lower()

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) / 4 ) ):
        x = int.from_bytes( keys[ i * 4:( i + 1 ) * 4 ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) / 4

def bptree_get_n_bad_style( root, key, n ):
    curr_level = root

    while True:
        data = ray.get( loader.get_object.remote( curr_level ) )
        keys = ray.get( loader.get_object.remote( get_entry( data, 0 ) ) )
        isleaf = keys[0] == 1
        keys = keys[1:]
        idx = upper_bound( keys, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys[ ( idx - 1 )* 4 : idx * 4 ], byteorder='little', signed=True ) == key ):

                leaf_nodes = []
                leaf_node = []

                for i in range( idx, int( len( data ) / 32 ) - 1 ):
                    leaf_node.append( loader.get_object.remote( get_entry( data, i ) ) )
                leaf_nodes.append( leaf_node )
                
                for i in range( 1, n ):
                    leaf_node = []
                    data = ray.get( loader.get_object.remote( get_entry( data, int( len( data ) / 32 ) - 1 ) ) )
                    for j in range( 1, int( len( data ) / 32 ) - 1 ):
                        leaf_node.append( loader.get_object.remote( get_entry( data, j ) ) )
                    leaf_nodes.append( leaf_node )

                leaf_data = []
                for i in range( 0, n ):
                    leaf_data.append( ray.get( leaf_nodes[i] ) )

                return leaf_data
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

@ray.remote
def bptree_get_leaf_nodes( acc, curr_level_data, n ):
    if len( curr_level_data ) == 0:
        return acc

    leaf_node = []
    for i in range( 1, int( len( curr_level_data ) / 32 ) - 1 ):
        leaf_node.append( loader.get_object.remote( get_entry( curr_level_data, i ) ) )
    acc.append( leaf_node )

    if len( acc ) >= n:
        return acc
    else:
        return bptree_get_leaf_nodes.remote( acc, loader.get_object.remote( get_entry( curr_level_data, int( len( curr_level_data ) / 32 ) - 1 ) ), n )


@ray.remote
def bptree_get_n_good_style( is_odd, curr_level_data, keys_data, key, n ):
    if is_odd:
        return bptree_get_n_good_style.remote( False, curr_level_data, loader.get_object.remote( get_entry( curr_level_data, 0 ) ), key, n )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ ( idx - 1 )* 4 : idx * 4 ], byteorder='little', signed=True ) == key ):
                leaf_nodes = []
                leaf_node = []

                for i in range( idx, int( len( curr_level_data ) / 32 ) - 1 ):
                    leaf_node.append( loader.get_object.remote( get_entry( curr_level_data, i ) ) )
                leaf_nodes.append( leaf_node )

                if n > 1:
                    return bptree_get_leaf_nodes.remote( leaf_nodes, loader.get_object.remote( get_entry( curr_level_data, int( len( curr_level_data ) / 32 ) - 1 ) ), n )
                else:
                    return leaf_nodes
            else:
                return "Not found"
        else:
            return bptree_get_n_good_style.remote( True, loader.get_object.remote( get_entry( curr_level_data, idx + 1 ) ), "", key, n )

@ray.remote
def bptree_get_n_good_style_collect( bptree_root, key, n ):
    ref = bptree_get_n_good_style.remote( True, loader.get_object.remote( bptree_root ), "", key, n )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )

    data = []
    for d in ref:
        data.append( ray.get( d ) )

    return data


bptree_root = os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) 

for i in range( 0, 10 ):
    start = time.time()
    bptree_get_n_bad_style( bptree_root, -337130320, n )
    end = time.time()
    print( end - start )

for i in range( 0, 10 ):
    start = time.time()
    ray.get( bptree_get_n_good_style_collect.remote( bptree_root, -337130320, n ) )
    end = time.time()
    print( end - start )
