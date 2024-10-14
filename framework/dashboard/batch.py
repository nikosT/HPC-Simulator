import yaml

import layouts.elements.run as r
#from layouts.elements.run import run_simulation


with open('config.yaml', 'r') as _f:
   config = yaml.load(_f, Loader=yaml.FullLoader)

#print(config)

r.run_simulation(config, config['queue_size'])

