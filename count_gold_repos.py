"""
Print the number of repositories stored in the gold layer.
Usage: python count_gold_repos.py [run_date]
  run_date: yyyy-mm-dd (default: today), or "cumulative" for cumulative gold
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold import count_gold_repositories

def main() -> None:
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    n = count_gold_repositories(run_date, cumulative=(run_date == "cumulative"))
    print(n)

if __name__ == "__main__":
    main()
