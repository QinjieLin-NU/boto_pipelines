import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--user', type=str, help='userId path', required=True)
parser.add_argument('--config', type=str, help='user config path', required=True)
args = parser.parse_args()

with open(args.config,'r') as f:
    x = f.readlines()

user=args.user
firstefs=True
with open(args.config,'w') as f:
    for i in x:
        if ('efs_path' in i) and firstefs: 
            i = '  efs_path: /%s/\n'%(user)
            firstefs = False
        if 'job_name' in i:
            i = 'job_name: job%s\n'%(user)
        f.write(i)