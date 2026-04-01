import os
import sys

from dotenv import load_dotenv

from agent import ask_agent


def main() -> None:
    load_dotenv()

    if not os.getenv("GOOGLE_API_KEY"):
        raise RuntimeError(
            "GOOGLE_API_KEY is not set. Add it to your environment or .env file before running the agent."
        )

    if len(sys.argv) < 2:
        raise SystemExit(
            'Usage: python src/main.py "What are the top categories by revenue?"'
        )

    question = " ".join(sys.argv[1:])
    answer = ask_agent(question)
    print(answer)


if __name__ == "__main__":
    main()
