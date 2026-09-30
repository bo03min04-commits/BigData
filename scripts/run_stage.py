"""실행 예: python scripts/run_stage.py --stage all --output outputs/lakehouse"""
import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from labkit.lakehouse import run_stage, run_pipeline

def main(default=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--stage', choices=['all', 'landing', 'bronze', 'silver', 'gold'], default=default or 'all')
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parents[1] / 'outputs/lakehouse')
    args = parser.parse_args()
    print(run_pipeline(args.output) if args.stage == 'all' else run_stage(args.stage, args.output))

if __name__ == '__main__':
    main()
