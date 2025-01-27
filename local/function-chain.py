import ray
import argparse
import time

parser = argparse.ArgumentParser("function-chain")
parser.add_argument("num_of_calls", help="number of callskey to lookup", type=int)
parser.add_argument("ray_address", help="address of ray", type=str)
args = parser.parse_args()

ray.init("ray://" + args.ray_address )

@ray.remote
def inc( input ):
    return input + 1;

ray.get( inc.remote( 0 ) )

start = time.monotonic()
ref = inc.remote( 0 )
for i in range( args.num_of_calls - 1 ):
    ref = inc.remote( ref )
ray.get( ref )

end = time.monotonic()

print( end - start )
