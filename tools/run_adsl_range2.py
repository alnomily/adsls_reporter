import argparse
import time

from scraper.runner import (
    process_adsl_range_to_accounts2,
    start_process_adsl_range_to_accounts2_background,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Process ADSL range and insert valid accounts into users_accounts2."
    )
    parser.add_argument("start", type=int, help="Start ADSL number")
    parser.add_argument("end", type=int, help="End ADSL number")
    parser.add_argument("network_id", type=int, help="Network ID")
    parser.add_argument("--threads", type=int, default=6, help="Max worker threads")
    parser.add_argument(
        "--save-account-data",
        action="store_true",
        help="Save account data via RPC after insert",
    )
    parser.add_argument(
        "--background",
        action="store_true",
        help="Run in a background thread and keep the process alive",
    )
    args = parser.parse_args()

    if args.background:
        thread = start_process_adsl_range_to_accounts2_background(
            start_adsl=args.start,
            end_adsl=args.end,
            network_id=args.network_id,
            max_workers=args.threads,
            save_account_data=args.save_account_data,
        )
        print(
            "Started background processing for range "
            f"{args.start}-{args.end} (threads={args.threads})."
        )
        try:
            while thread.is_alive():
                time.sleep(2)
        except KeyboardInterrupt:
            print("Stopping... (background thread will exit when complete)")
        return 0

    result = process_adsl_range_to_accounts2(
        start_adsl=args.start,
        end_adsl=args.end,
        network_id=args.network_id,
        max_workers=args.threads,
        save_account_data=args.save_account_data,
    )
    print("Done.")
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
