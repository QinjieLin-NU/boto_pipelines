from pipelines.flow_v1 import build_flow

if __name__ == "__main__":
    flow = build_flow()
    # flow.visualize()
    flow.run()