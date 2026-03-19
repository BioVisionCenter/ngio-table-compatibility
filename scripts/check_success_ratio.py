import csv
import sys

EXPECTED_FAILURES = 12  # baseline from check_results.csv as of 2026-03-19


def main():
    with open("check_results.csv") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    failures = sum(1 for r in rows if r["status"] == "failure")
    successes = total - failures
    ratio = successes / total if total > 0 else 0.0

    print(f"Total checks : {total}")
    print(f"Successes    : {successes}")
    print(f"Failures     : {failures}")
    print(f"Success ratio: {ratio:.1%}")
    print(f"Expected max failures: {EXPECTED_FAILURES}")

    if failures > EXPECTED_FAILURES:
        print(f"\nFAIL: {failures} failures exceed expected maximum of {EXPECTED_FAILURES}")
        sys.exit(1)

    print(f"\nOK: {failures} failures are within the expected maximum of {EXPECTED_FAILURES}")


if __name__ == "__main__":
    main()
