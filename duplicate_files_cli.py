import os
import sys
import shutil
import zipfile
import uuid
import json
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from tqdm import tqdm

# ---------------- Utils ----------------
def ask(prompt, help_text=None, default=None):
    if help_text:
        print(f"\nℹ️  {help_text}")
    value = input(f"{prompt} ")
    if not value and default is not None:
        return default
    return value

def error(msg):
    print(f"\n❌ ERROR: {msg}")
    sys.exit(1)

def to_bool(v):
    return v.lower() in ["y", "yes", "true", "1"]

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

# ---------------- Interactive Inputs ----------------
print("\n=== 📁 File Duplication Tool (Pure Python) ===")

source_files = ask(
    "Source files (comma-separated paths):",
    "Example: demo-file/countries.json, assets/config.yaml",
)

sources = [s.strip() for s in source_files.split(",") if s.strip()]
if not sources:
    error("No source files provided")

for src in sources:
    if not os.path.isfile(src):
        error(f"File not found: {src}")

copies = ask(
    "How many copies per source file?",
    "Must be a number > 0",
)
if not copies.isdigit() or int(copies) <= 0:
    error("Invalid copies value")
copies = int(copies)

target_folder = ask(
    "Target output folder name:",
    "Folder will be created if it doesn't exist",
)

per_subfolder = ask(
    "Files per subfolder (0 = no subfolders):",
    "Example: 5000",
    default="0",
)
if not per_subfolder.isdigit():
    error("per_subfolder must be numeric")
per_subfolder = int(per_subfolder)

workers = ask(
    "Parallel workers:",
    "Recommended: CPU cores (e.g. 4 or 8)",
    default="4",
)
if not workers.isdigit() or int(workers) <= 0:
    error("Invalid worker count")
workers = int(workers)

dry_run = to_bool(
    ask("Dry run? (y/n):", "No files will be written", default="n")
)

resume_mode = to_bool(
    ask("Resume mode? (y/n):", "Skips already created files", default="y")
)

randomize = to_bool(
    ask("Randomize filenames? (y/n):", "Uses UUID filenames", default="n")
)

zip_output = to_bool(
    ask("Create ZIP output? (y/n):", "Creates zip files at the end", default="y")
)

chunk_size = ask(
    "ZIP chunk size:",
    "Files per zip (default 5000)",
    default="5000",
)
if not chunk_size.isdigit():
    error("chunk_size must be numeric")
chunk_size = int(chunk_size)

max_limit = ask(
    "Maximum allowed total files:",
    "Safety limit (default 50000)",
    default="50000",
)
if not max_limit.isdigit():
    error("max_limit must be numeric")
max_limit = int(max_limit)

# ---------------- Limits ----------------
total_files = copies * len(sources)
if total_files > max_limit:
    error(f"Requested {total_files} files exceeds limit {max_limit}")

print(f"\n📊 Total files to generate: {total_files}")

# ---------------- Resume ----------------
os.makedirs(target_folder, exist_ok=True)
STATE_FILE = os.path.join(target_folder, ".resume_state.json")
completed = set()

if resume_mode and os.path.exists(STATE_FILE):
    with open(STATE_FILE) as f:
        completed = set(json.load(f))
    print(f"🔁 Resume enabled — {len(completed)} files already done")

# ---------------- Build Tasks ----------------
tasks = []

for src in sources:
    base = os.path.basename(src)
    name, ext = os.path.splitext(base)

    for i in range(1, copies + 1):
        folder = target_folder
        if per_subfolder > 0:
            folder = os.path.join(
                target_folder, f"part_{((i - 1) // per_subfolder) + 1}"
            )

        os.makedirs(folder, exist_ok=True)

        filename = (
            f"{uuid.uuid4().hex}{ext}"
            if randomize
            else f"{name}_{i}{ext}"
        )

        dest = os.path.join(folder, filename)

        if resume_mode and dest in completed:
            continue

        tasks.append((src, dest))

# ---------------- Copy ----------------
def copy_task(task):
    src, dest = task
    if not dry_run:
        shutil.copy2(src, dest)
    completed.add(dest)

with ThreadPoolExecutor(max_workers=workers) as executor:
    list(
        tqdm(
            executor.map(copy_task, tasks),
            total=len(tasks),
            desc="Generating files",
            unit="files",
        )
    )

if resume_mode and not dry_run:
    with open(STATE_FILE, "w") as f:
        json.dump(list(completed), f)

# ---------------- ZIP ----------------
if zip_output and not dry_run:
    all_files = []
    for root, _, files in os.walk(target_folder):
        for f in files:
            if not f.endswith(".json"):
                all_files.append(os.path.join(root, f))

    for idx, chunk in enumerate(chunked(all_files, chunk_size), start=1):
        zip_name = f"{target_folder}_part{idx}.zip"
        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as z:
            for file in chunk:
                z.write(file, arcname=os.path.relpath(file, target_folder))
        print(f"📦 Created {zip_name}")

print("\n✅ DONE")
