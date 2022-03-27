import csv
import argparse

parser = argparse.ArgumentParser(description='Workflow example')
parser.add_argument('--input_path', type=str, default="./data/values.csv")
parser.add_argument('--output_path', type=str, default="./data/tvalues.csv")
args = parser.parse_args()

def preprocess_data(input_path, output_path):
    with open(input_path, 'r') as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    
    tdata = [i+1 for i in data]
    
    with open(output_path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(tdata)
    return tdata

if __name__ == "__main__":
    preprocess_data(input_path=args.input_path, output_path=args.output_path)