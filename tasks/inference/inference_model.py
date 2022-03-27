import csv 
import argparse

parser = argparse.ArgumentParser(description='Workflow example')
parser.add_argument('--model_path', type=str, default="./data/model.csv")
args = parser.parse_args()

def test(model_path):
    with open(model_path, 'r') as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    
    sum_data = sum(data)
    print("testing result:", sum_data)
    return sum_data

if __name__ == "__main__":
    test(model_path=args.model_path)