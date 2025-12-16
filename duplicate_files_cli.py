#!/usr/bin/env python3
"""
duplicate_files_cli.py

Pure Python file duplication tool with:
- Interactive mode OR argparse flags
- Parallel generation
- Progress bar with ETA & files/sec
- Resume support
- Subfolder splitting
- ZIP output with chunking
- Dry-run mode
- Safety limits
- Multiple source files
"""

import os
import sys
import shutil
import json
import zipfile
import uuid
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

RESUME_FILE = ".resume_state.json"


# -----------------------------
# Utilities
# -----------------------------
def die(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def is_positive_int(value):
    try:
        iv = int(value)
        return iv >= 0
    except Exception:
        return False


def load_resume(output_dir):
    path = output_dir / RESUME_FILE
    if path.exists():
        with open(path, "r") as f:
            return set(json.load(f))
    return set()


def save_resume(output_dir, completed):
    path = output_dir / RESUME_FILE
    with open(path, "w") as f:
        json.dump(sorted(completed), f)


# -----------------------------
# File generation
# -----------------------------
def copy_one(src, dst):
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return dst.name


def generate_files(
    sources,
    copies,
    output_dir,
    per_subfolder,
    workers,
    dry_run,
    resume,
    randomize,
    max_limit,
):
    completed = load_resume(output_dir) if resume else set()
    tasks = []

    total = len(sources) * copies
    if total > max_limit:
        die(f"Requested {total} files exceeds limit {max_limit}")

    for src in sources:
        src = Path(src)
        if not src.exists():
            die(f"Source not found: {src}")

        stem = src.stem
        suffix = src.suffix

        for i in range(1, copies + 1):
            name = (
                f"{stem}_{uuid.uuid4().hex}{suffix}"
                if randomize
                else f"{stem}_{i}{suffix}"
            )

            if name in completed:
                continue

            if per_subfolder > 0:
                idx = (i - 1) // per_subfolder + 1
                dst = output_dir / f"part_{idx}" / name
            else:
                dst = output_dir / name

            tasks.append((src, dst, name))

    if dry_run:
        print(f"[DRY-RUN] Would generate {len(tasks)} files")
        return completed

    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {
            exe.submit(copy_one, src, dst): name
            for src, dst, name in tasks
        }

        with tqdm(total=len(futures), unit="file") as bar:
            for fut in as_completed(futures):
                name = futures[fut]
                fut.result()
                completed.add(name)
                bar.update(1)

    if resume:
        save_resume(output_dir, completed)

    return completed


# -----------------------------
# ZIP chunking
# -----------------------------
def zip_chunks(output_dir, chunk_size):
    files = sorted(
        p for p in output_dir.rglob("*")
        if p.is_file() and p.name != RESUME_FILE
    )

    if not files:
        return

    for i in range(0, len(files), chunk_size):
        zip_name = f"{output_dir.name}_part{i//chunk_size+1}.zip"
        zip_path = output_dir.parent / zip_name

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            for f in files[i:i + chunk_size]:
                z.write(f, f.relative_to(output_dir))


# -----------------------------
# Interactive prompts
# -----------------------------
def prompt(msg, validator=None, default=None):
    while True:
        v = input(f"{msg} [{default}]: " if default else f"{msg}: ").strip()
        if not v and default is not None:
            return default
        if validator is None or validator(v):
            return v
        print("Invalid value, try again.")


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Duplicate files at scale")
    parser.add_argument("--sources", help="Comma-separated source files")
    parser.add_argument("--copies", type=int, help="Copies per source")
    parser.add_argument("--output", help="Output directory")
    parser.add_argument("--per-subfolder", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--randomize", action="store_true")
    parser.add_argument("--zip", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--max-limit", type=int, default=50000)

    args = parser.parse_args()

    # Interactive fallback
    if not args.sources:
        args.sources = prompt(
            "Source file(s) (comma-separated)", lambda x: True
        )
        args.copies = int(prompt("Copies per source", is_positive_int))
        args.output = prompt("Output directory")
        args.per_subfolder = int(prompt("Files per subfolder (0 = none)", is_positive_int, 0))
        args.workers = int(prompt("Parallel workers", is_positive_int, 4))
        args.dry_run = prompt("Dry-run? (y/n)", lambda x: x.lower() in ["y", "n"], "n") == "y"
        args.resume = prompt("Resume mode? (y/n)", lambda x: x.lower() in ["y", "n"], "n") == "y"
        args.randomize = prompt("Randomize filenames? (y/n)", lambda x: x.lower() in ["y", "n"], "n") == "y"
        args.zip = prompt("Zip output? (y/n)", lambda x: x.lower() in ["y", "n"], "n") == "y"
        args.chunk_size = int(prompt("ZIP chunk size", is_positive_int, 5000))
        args.max_limit = int(prompt("Max safety limit", is_positive_int, 50000))

    sources = [s.strip() for s in args.sources.split(",")]
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    completed = generate_files(
        sources=sources,
        copies=args.copies,
        output_dir=output_dir,
        per_subfolder=args.per_subfolder,
        workers=args.workers,
        dry_run=args.dry_run,
        resume=args.resume,
        randomize=args.randomize,
        max_limit=args.max_limit,
    )

    if args.zip and not args.dry_run:
        zip_chunks(output_dir, args.chunk_size)

    print(f"SUCCESS: {len(completed)} files processed")


if __name__ == "__main__":
    main()
