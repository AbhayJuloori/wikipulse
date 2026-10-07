from __future__ import annotations

import argparse

from wikipulse.ingest import ingest_forever, replay


def main():
    parser = argparse.ArgumentParser(description="WikiPulse operations")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("ingest")
    replay_command = commands.add_parser("replay")
    replay_command.add_argument("path")
    replay_command.add_argument(
        "--retime", action="store_true", help="shift fixture event times to now"
    )
    args = parser.parse_args()
    if args.command == "ingest":
        ingest_forever()
    else:
        replay(args.path, retime=args.retime)


if __name__ == "__main__":
    main()
