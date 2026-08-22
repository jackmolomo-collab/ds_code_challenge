import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from outputs.result_writer import ResultWriter


result = {
    "source": "city-hex-polygons-8-10.geojson",
    "total": 203840,
    "valid": 3832,
    "invalid": 200008,
    "score": 0.01879905808477237,
    "passed": False
}


writer = ResultWriter()

output_file = writer.write_json(
    result,
    "validation_result.json"
)

print(f"Output written to: {output_file}")