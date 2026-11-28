"""Fail unless a pytest JUnit report holds at least one test that ran (an all-skipped run exits 0 in pytest)."""

import sys
import xml.etree.ElementTree as ET


def main(path: str) -> int:
    cases = ET.parse(path).getroot().iter("testcase")
    ran = sum(1 for case in cases if case.find("skipped") is None)
    if ran == 0:
        print(f"{path}: no integration test ran (all skipped or none collected)")
        return 1
    print(f"{path}: {ran} integration tests ran")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
