import os
import shutil


def wipe_dir(path: str) -> None:
    if not os.path.exists(path):
        print(f"Skip: {path} (not found)")
        return
    for name in os.listdir(path):
        full = os.path.join(path, name)
        try:
            if os.path.isdir(full):
                shutil.rmtree(full)
            else:
                os.remove(full)
        except Exception as exc:
            print(f"Failed to remove {full}: {exc}")
    print(f"Wiped: {path}")


def main() -> None:
    # Full reset: deletes all collected data (snapshots + raw events + backfill)
    wipe_dir("data")
    wipe_dir("data_backfill")


if __name__ == "__main__":
    main()
