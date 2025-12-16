# File Duplication Tool


## Overview
A **high‑performance, resumable file duplication utility** written in pure Python.


It supports:
- Parallel file generation
- Progress bar with ETA & speed
- Resume after interruption
- Chunked ZIP outputs
- Randomized or deterministic filenames
- Safety limits
- Interactive mode **and** non‑interactive CLI flags


---


## Requirements
- Python **3.9+**
- One external dependency:

```txt
tqdm>=4.66.0
```

## Virtual Environment (Recommended)
Linux / macOS
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Windows (PowerShell)
```
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
To deactivate:
```
deactivate
```
## ▶ Usage
Interactive Mode (Guided)
```
python duplicate_files_cli.py
```
You will be prompted for each option with help text.

Non‑Interactive Mode (Argparse)
```
python duplicate_files_cli.py \
  --sources demo-file/countries.json \
  --copies 20000 \
  --output countries \
  --per-subfolder 5000 \
  --workers 4 \
  --zip \
  --resume
````
## ⚙️ CLI Arguments
Flag	Description	Default
```
--sources	Comma‑separated source files	required
--copies	Copies per source file	required
--output	Target output folder	required
--per-subfolder	Files per subfolder (0 = none)	0
--workers	Parallel workers	4
--dry-run	Validate only	off
--resume	Resume interrupted runs	off
--randomize	Random UUID filenames	off
--zip	Create ZIP output	off
--chunk-size	Files per ZIP	5000
--max-limit	Safety file limit	50000
```
## 📊 Example Output
```
Generating files: 65%|██████▌ | 13000/20000 [00:07<00:04, 1850 files/s]
```
- Shows progress
- Shows ETA
- Shows throughput

## 📦 Output Structure Example
```
countries/
├── part_1/
│   ├── countries_1.json
│   └── ...
├── part_2/
└── .resume_state.json
countries_part1.zip
countries_part2.zip
```

## 🔁 Resume Mode

If execution is interrupted:
```
python duplicate_files_cli.py --resume ...
```
Already‑generated files are skipped safely.

## 🛑 Safety Notes

- Default limit: 50,000 files
- Use ZIP artifacts for large outputs
- Prefer dry‑run before big jobs

## 🧠 Use Cases
- Dataset generation
- Load testing
- CI/CD artifacts
- File‑system stress tests

## ✅ License

MIT – use freely
