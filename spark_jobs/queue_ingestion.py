from pathlib import Path
import shutil


def move_files_from_queue(queue_path: str, raw_path: str):
    queue = Path(queue_path)
    raw = Path(raw_path)

    raw.mkdir(parents=True, exist_ok=True)

    files = list(queue.glob("*"))

    for file in files:
        target = raw / file.name
        shutil.move(str(file), str(target))

    print(f"Moved {len(files)} files from queue to raw")