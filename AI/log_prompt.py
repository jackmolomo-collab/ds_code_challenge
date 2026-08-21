from datetime import datetime
from pathlib import Path
import argparse

ROOT = Path(__file__).resolve().parent.parent

AI_LOG = ROOT / "AI_log.md"
PROMPT_DIR = ROOT / "ai" / "prompts"


def log_prompt(
    tool,
    model,
    area,
    prompt,
    tokens,
    response_used
):

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    area_dir = PROMPT_DIR / area
    area_dir.mkdir(parents=True, exist_ok=True)

    filename = datetime.now().strftime("%Y%m%d_%H%M%S.md")

    prompt_file = area_dir / filename

    prompt_file.write_text(
        f"""# AI Interaction

**Date:** {timestamp}

**Tool:** {tool}

**Model:** {model}

**Area:** {area}

**Tokens:** {tokens}

**Response Used:** {response_used}

## Prompt

{prompt}
""",
        encoding="utf-8"
    )

    if not AI_LOG.exists():

        AI_LOG.write_text(
            """# AI Usage Log

This file records AI-assisted development undertaken during the assessment.

| Date | Tool | Model | Area | Tokens | Response Used | Prompt File |
|---|---|---|---|---:|---|---|
""",
            encoding="utf-8"
        )

    with AI_LOG.open("a", encoding="utf-8") as file:

        file.write(
            f"| {timestamp} | {tool} | {model} | "
            f"{area} | {tokens} | {response_used} | "
            f"`{prompt_file.relative_to(ROOT)}` |\n"
        )

    print(f"AI interaction logged: {prompt_file}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--tool", required=True)

    parser.add_argument("--model", required=True)

    parser.add_argument("--area", required=True)

    parser.add_argument("--prompt", required=True)

    parser.add_argument("--tokens", required=True, type=int)

    parser.add_argument(
        "--response-used",
        action="store_true"
    )

    args = parser.parse_args()

    log_prompt(
        tool=args.tool,
        model=args.model,
        area=args.area,
        prompt=args.prompt,
        tokens=args.tokens,
        response_used=args.response_used
    )