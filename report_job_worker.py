import argparse

from report_jobs import fail_report_job, process_report_job


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=int, required=True)
    args = parser.parse_args()

    try:
        processed = process_report_job(args.job_id)
        return 0 if processed else 1
    except Exception as exc:
        fail_report_job(args.job_id, str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
