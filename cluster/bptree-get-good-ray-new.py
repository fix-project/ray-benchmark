import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "fix_path", help="path to .fix repository", type=str)
parser.add_argument( "key_list", help="path to list of keys", type=str)
parser.add_argument( "num_of_keys", help="the number of keys to get", type=int)
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

@ray.remote
class Loader:
    def __init__( self ):
        # prefix_map maps from handle[:48] to handle[48:]
        self.prefix_map = {}
        self.buffer = {}
        for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
            self.prefix_map[filename[:48]] = filename[48:]

    def get_object( self, handle ):
        raw = base64.b16decode( handle.upper() )
        if raw[30] | 0b11111000 == 0b11111000:
            size = raw[30] >> 3
            return raw[:size] 

        prefix = handle[:48]
        filename = prefix + self.prefix_map[prefix]

        if filename in self.buffer:
            return self.buffer[filename]

        with open( os.path.join( fix_path, "data/", filename ), 'rb') as file:
            data = file.read()
        self.buffer[filename] = data
        return data

    # Return list of prefixes that are at this loader
    def keys( self ):
        return self.prefix_map.keys()

    def set_up( self, key_to_loader_map, index ):
        self.key_to_loader_map = key_to_loader_map
        self.index = index

    def get_loader_index( self, handle ):
        if handle[:48] in key_to_loader_map:
            return key_to_loader_map[handle[:48]]
        else:
            return self.index

    #async def get_remote_object( self, handle ):
    #    if handle[:48] in key_to_loader_map:
    #        # Send request to the loader with this key
    #        loader_index = key_to_loader_map[handle[:48]]
    #        ref = self.loaders[loader_index].get_object.remote( handle )
    #        result = await ref
    #        return result
    #    else:
    #        # Send request to local loader
    #        return self.get_object( handle )

# Create an actor
loaders = []
loader_index = 0;
key_to_loader_map = {}
for node in nodes:
    loader = Loader.options(resources={ node : 0.0001 }).remote()
    keys = ray.get( loader.keys.remote() )
    for key in keys:
        key_to_loader_map[key] = loader_index
    loaders.append( loader )
    loader_index += 1

loader_index = 0
for loader in loaders:
    ray.get( loader.set_up.remote( key_to_loader_map, loader_index ) )
    loader_index = loader_index + 1

@ray.remote
def get_object_from_loader( handle, loader_index ):
    return loaders[loader_index].get_object.remote( handle )

def get_object( handle ):
    local_loader_index = nodes.index( "node:" + ray._private.services.get_node_ip_address() )
    return get_object_from_loader.remote( handle, loaders[local_loader_index].get_loader_index.remote( handle ) )

def get_entry( data, i ):
    return base64.b16encode( data[ int(i) * 32: int( i + 1 ) *32 ] ).decode("utf-8").lower()

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) / 4 ) ):
        x = int.from_bytes( keys[ int(i * 4):int(( i + 1 ) * 4) ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) / 4

@ray.remote
def bptree_get_good_style( step, curr_level_data, keys_data, key ):
    if step == 0:
        return bptree_get_good_style.remote( 1, curr_level_data, "", key )
    elif step == 1:
        return bptree_get_good_style.remote( 2, curr_level_data, get_object( get_entry( curr_level_data, 0 ) ), key )
    elif step == 2:
        return bptree_get_good_style.remote( 3, curr_level_data, keys_data, key )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                return get_object( get_entry( curr_level_data, idx ) )
            else:
                return "Not found"
        else:
            return bptree_get_good_style.remote( 0, get_object( get_entry( curr_level_data, idx + 1 ) ), "", key )

@ray.remote
def bptree_get_good_style_collect( bptree_root, key ):
    ref = bptree_get_good_style.remote( 0, get_object( bptree_root ), "", key )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ref

bptree_root = os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) 

refs = []
for key in key_list:
    refs.append( bptree_get_good_style_collect.remote( bptree_root, key ) )

ray.get( refs )
