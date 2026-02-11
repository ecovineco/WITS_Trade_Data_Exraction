# WITS Trade Extractor (HS6) — README

## Overview
This script downloads international trade data from the World Bank WITS download endpoint and produces a consolidated dataset by:

- **Reporter** (EU + selected countries)
- **Partner** (6 selected partners + **Rest of World** + derived **European Union** rows)
- **Year** (2020–2024)
- **Trade flow** (Import / Export)
- **Quantity (kg)** and **Trade Value (USD, EUR)**

Outputs are saved as both **CSV** and **Excel**.

## What you need to edit
Only one line is intended to be edited, the first line in the code, defining the array ```PRODUCTS```

Example:
```python
PRODUCTS = ["430310", "430310"]
```
- Each entry must be an HS6 product code (6 digits).
- If you list multiple codes, the script adds their quantities/values together (per reporter/partner/year/flow).