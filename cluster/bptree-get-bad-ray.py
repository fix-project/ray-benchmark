import base64
import argparse
import os
import time 
import boto3

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "fix_path", help="path to .fix repository", type=str)
parser.add_argument( "tree_root", help="name of root of tree", type=str)
parser.add_argument( "key_list", help="path to list of keys", type=str)
parser.add_argument( "num_of_keys", help="the number of keys to get", type=int)
parser.add_argument( "minio_port", help="port to minio client", type=int)
args = parser.parse_args()

import ray
ray.init()

def get_object( handle ):
    raw = base64.b16decode( handle.upper() )
    if raw[30] | 0b11111000 == 0b11111000:
        size = raw[30] >> 3
        return raw[:size] 

    s3_target = boto3.resource('s3', 
                               endpoint_url='http://localhost:' + str( args.minio_port ) ,
                               aws_access_key_id='minioadmin',
                               aws_secret_access_key='minioadmin',
                               aws_session_token=None,
                               config=boto3.session.Config(signature_version='s3v4'),
                               verify=False )
    obj = s3_target.Object( args.fix_path, str( handle[:48] ) )
    return obj.get()['Body'].read()

def get_entry( data, i ):
    return base64.b16encode( data[ int(i) * 32: int( i + 1 ) *32 ] ).decode("utf-8").lower()

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) / 4 ) ):
        x = int.from_bytes( keys[ int(i * 4):int(( i + 1 ) * 4) ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) / 4

bptree_root = args.tree_root

@ray.remote
def bptree_get_bad_style( root, key ):
    psutil.Process().cpu_affinity( [cpuid] )
    curr_level = root

    while True:
        data = get_object( curr_level )
        keys = get_object( get_entry( data, 0 ) )
        isleaf = keys[0] == 1
        keys = keys[1:]
        idx = upper_bound( keys, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                return ray.get( get_object( get_entry( data, idx ) ) )
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

refs = []
for key in key_list:
    refs.append( bptree_get_bad_style.remote( bptree_root, key ) )

ray.get( refs )
