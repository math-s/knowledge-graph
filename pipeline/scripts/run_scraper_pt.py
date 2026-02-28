"""CLI entry point for the Portuguese CCC scraper."""

import logging
import sys
from pathlib import Path

# Ensure the project root is on the path so pipeline.src imports work
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.src.scraper_pt import run  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

if __name__ == "__main__":
    run()
