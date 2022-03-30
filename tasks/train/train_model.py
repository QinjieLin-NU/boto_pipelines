import csv 
import argparse

parser = argparse.ArgumentParser(description='Workflow example')
parser.add_argument('--data_path', type=str, default="./data/tvalues.csv")
parser.add_argument('--model_path', type=str, default="./data/model.csv")
args = parser.parse_args()

def train(data_path,model_path):
    print("training model...")
    with open(data_path, 'r') as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    
    model_data = [i*2 for i in data]
    
    with open(model_path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(model_data)
    return model_data

if __name__ == "__main__":
    train(data_path=args.data_path,model_path=args.model_path)