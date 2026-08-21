from datetime import datetime
from pathlib import Path
import argparse

ROOT = Path(__file__).resolve().parent.parent
AI_LOG = ROOT / "AI.md"
PROMPT_DIR = ROOT / "ai" / "prompts"


def log_prompt(tool: str, area: str, prompt: str, response_used: bool):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    area_dir = PROMPT_DIR / area
    area_dir.mkdir(parents=True, exist_ok=True)

    filename = datetime.now().strftime("%Y%m%d_%H%M%S.md")
    prompt_file = area_dir / filename

    prompt_file.write_text(
        f"""# AI Interaction

**Timestamp:** {timestamp}

**Tool:** {tool}

**Area:** {area}

**Response Used:** {response_used}

## Prompt

{prompt}
""",
        encoding="utf-8",
    )

    if not AI_LOG.exists():
        AI_LOG.write_text(
            "# AI Usage Log\n\n"
            "| Timestamp | Tool | Area | Response Used | Prompt File |\n"
            "|---|---|---|---|---|\n",
            encoding="utf-8",
        )

    with AI_LOG.open("a", encoding="utf-8") as f:
        f.write(
            f"| {timestamp} | {tool} | {area} | "
            f"{response_used} | `{prompt_file.relative_to(ROOT)}` |\n"
        )

    print(f"Logged AI interaction: {prompt_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--tool", required=True)
    parser.add_argument("--area", required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument(
        "--response-used",
        action="store_true",
    )

    args = parser.parse_args()

    log_prompt(
        tool=args.tool,
        area=args.area,
        prompt=args.prompt,
        response_used=args.response_used,
    )