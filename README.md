# README

## About

This repository contains scripts and one jupyter notebook to execute benchmarks for the [signac core](http://www.signac.io) application.
In the current version, they are optionally compared against [datreant](http://datreant.org/).

Author: Carl Simon Adorf, csadorf@umich.edu

All code within this repository, except for the `tempdir.py` module, are distributed under the MIT License.
The full license text is in the `LICENSE.txt` file.

## Quickstart

To run benchmarks, first clone this repository, then execute:
```bash
./run.sh
```
This will generate a collection file called `benchmark.txt`, which contains the benchmark results.

To visualize and plot the benchmark results, execute the `report.ipynb` jupyter notebook.

## Usage

The `run_benchmark.py` script is the main benchmark execution script.
Execute `python run_benchmark.py --help` to get help on how to execute benchmarks.
For example, to run a benchmark for signac for a dataspace with 1,000 directories in the `/tmp` directory, you would execute:
```bash
python run_benchmark.py signac -N 1000 --root=/tmp
```
