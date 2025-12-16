import os
import sys
import shutil
import zipfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# -------------------- Utils --------------------
def error(msg):
    print(f"ERROR: {msg}")
    sys.exit(1)

def to_bool(v):
    return v.lower() == "true"

# -------------------- Args --------------------
if len(sys.argv) != 11:
    error("Invalid arguments")

(
    source_files,
    copies,
    target_folder,
    per_subfolder,
    workers,
    dry_run,
    zip_output,
    randomize,
    max_limit,
) = sys.argv[1:]

# -------------------- Validation --------------------
sources = [s.strip() for s in source_files.split(",") if s.strip()]
if not sources:
    error("No source files provided")

for src in sources:
    if not os.path.isfile(src):
        error(f"Source file not found: {src}")

if not copies.isdigit() or int(copies) <= 0:
    error("copies must be > 0")

if not per_subfolder.isdigit():
    error("per_subfolder must be numeric")

if not workers.isdigit() or int(workers) <= 0:
    error("parallel_workers must be > 0")

if not max_limit.isdigit():
    error("max_files_limit must be numeric")

copies = int(copies)
per_subfolder = int(per_subfolder)
workers = int(workers)
max_limit = int(max_limit)

dry_run = to_bool(dry_run)
zip_output = to_bool(zip_output)
randomize = to_bool(randomize)

# -------------------- Limits --------------------
total_files = copies * len(sources)
if total_files > max_limit:
    error(f"Requested {total_files} files exceeds limit {max_limit}")

if dry_run:
    print("DRY RUN ENABLED — no files will be written")

os.makedirs(target_folder, exist_ok=True)

# -------------------- Build tasks --------------------
tasks = []

for src in sources:
    base = os.path.basename(src)
    name, ext = os.path.splitext(base)

    for i in range(1, copies + 1):
        folder = target_folder
        if per_subfolder > 0:
            folder = os.path.join(
                target_folder,
                f"part_{((i - 1) // per_subfolder) + 1}",
            )

        os.makedirs(folder, exist_ok=True)

        filename = (
            f"{uuid.uuid4().hex}{ext}"
            if randomize
            else f"{name}_{i}{ext}"
        )

        dest = os.path.join(folder, filename)
        tasks.append((src, dest))

# -------------------- Copy with progress --------------------
def copy_task(task):
    src, dest = task
    if not dry_run:
        shutil.copy2(src, dest)

with ThreadPoolExecutor(max_workers=workers) as executor:
    list(
        tqdm(
            executor.map(copy_task, tasks),
            total=len(tasks),
            desc="Generating files",
            unit="files",
        )
    )

# -------------------- Zip output --------------------
if zip_output and not dry_run:
    zip_path = f"{target_folder}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(target_folder):
            for f in files:
                full = os.path.join(root, f)
                zipf.write(full, arcname=os.path.relpath(full, target_folder))

print("SUCCESS")
print(f"Files created: {total_files}")
