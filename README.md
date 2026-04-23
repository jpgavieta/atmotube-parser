# atmotube-parser
Python-based pipeline to parse Atmotube data (from PRO or PRO 2) using keyword-based detection into subdivided data frames (or save as a merged CSV), ready for spatial and/or time series analysis.

Features:
- Keyword-based column detection (robust to messy exports)
- Timezone and local time inference from GPS coordinates
- Breakdown of each sensor and variable used by Atmotube PRO/PRO2, includes notes and reference sources

## How to use
1. Upload raw CSV file to 1.upload
2. Run the .py script in 2.make
3. Save the parsed CSV to 3.save

Example data taken from [https://support.atmotube.com/en/articles/13002682-history-mode-overview](https://support.atmotube.com/en/articles/13002682-history-mode-overview)
