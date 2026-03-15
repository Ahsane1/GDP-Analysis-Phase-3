# Phase 3: Generic Concurrent Real-Time Pipeline

## Overview
This project implements a fully generalized, concurrent data-processing pipeline using Python's `multiprocessing` library. It features a Producer-Consumer architecture, Cryptographic Signature Verification, a Functional Core with an Imperative Shell, and a Real-Time Telemetry Dashboard utilizing the Observer Design Pattern.

## Execution Instructions

**1. Main Execution File**
The central orchestrator for this pipeline is `main.py`. This is the only file you need to run to execute the entire system.

```bash
python main.py
```

**2. Configuration File Placement**
The pipeline is strictly driven by `config.json`. This file must be placed directly in the root directory alongside `main.py`.

**3. Dataset Placement**
Place the target dataset (e.g., `sample_sensor_data.csv` or `unseen_climate_data.csv`) inside the `data/` directory. Ensure the path mapped in `config.json` matches this location (e.g., `"dataset_path": "data/sample_sensor_data.csv"`).

## Directory Structure Requirement
Ensure the project structure is maintained as follows for successful module imports:
* `main.py` (Orchestrator)
* `config.json` (System Configuration)
* `interface.py` (Abstract Subject/Observer Classes)
* `telemetry.py` (Dashboard and Telemetry Monitors)
* `Readme.md` (Project Documentation)
* `plugins/`
  * `__init__.py`
  * `input.py` (GenericCSVReader)
* `core/`
  * `__init__.py`
  * `worker.py` (CoreWorker, Gatherer, and Functional Core logic)
* `data/`
  * `sample_sensor_data.csv`

## Dependencies
* `matplotlib` (for real-time dashboard rendering)
* Standard Python 3.x libraries (`csv`, `multiprocessing`, `json`, `hashlib`, `queue`, `os`, `sys`, `time`)