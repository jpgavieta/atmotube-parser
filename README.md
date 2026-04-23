# atmotube-parser
Python-based pipeline to parse Atmotube data (from PRO or PRO 2) using keyword-based detection into subdivided data frames (or save as a merged CSV), ready for spatial and/or time series analysis.

Features:
- Keyword-based column detection (robust to messy exports)
- Timezone and local time inference from GPS coordinates
- Breakdown of each sensor and variable used by Atmotube PRO/PRO2, includes notes and reference sources

## How to use
1. Upload raw CSV file to 1.upload
2. Install dependencies:
```bash
pip install pandas tzfpy
```
or 
```bash
conda install -c conda-forge pandas tzfpy
```
4. Follow intructions in the 2.make
```
# Open a terminal in VS Code (Ctrl+Shift+`)
# Set working directory to current directory `cd .`
# Test run this script as `python 2.make/atmoData.py example_ext.csv`
# Run it with your actual csv.
```
6. Automatically saves the parsed CSV to 3.save

Example data taken from [https://support.atmotube.com/en/articles/13002682-history-mode-overview](https://support.atmotube.com/en/articles/13002682-history-mode-overview)
