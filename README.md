# Standalone CLMS HR-VPP ST downloader

By default it retrieves both PPI and QFLAG in LAEA

## 1. Create the environment

Open Terminal in this folder and run:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## 2. Download by CLMS tile code

```bash
python3 download_hrvpp_st.py \
  --username YOUR_WEKEO_USERNAME \
  --destination /absolute/path/to/hrvpp_st \
  --start-date 2023-01-01 \
  --end-date 2023-12-31 \
  --tiles E44N24
```

Asks for the WEkEO password without displaying it. Multiple
tiles can be supplied after `--tiles`, for example:

```bash
--tiles E44N24 E45N24 E45N23
```

## 3. Download by WGS84 bounding box

Use longitude and latitude in the order west, south, east, north:

```bash
python3 download_hrvpp_st.py \
  --username YOUR_WEKEO_USERNAME \
  --destination /absolute/path/to/hrvpp_st \
  --start-date 2023-01-01 \
  --end-date 2023-12-31 \
  --bbox -5.19125 41.158861 -5.110642 41.20293
```

## Options

- `--products PPI QFLAG` downloads both products and is the default.
- `--products PPI` or `--products QFLAG` downloads only one product type.
- `--workers 4` controls parallel transfers; use 1 to 4 if WEkEO is unstable.
- `--downloads-per-hour 500` applies a restart-safe local HDA allowance. If
  WEkEO has formally assigned a different allowance to the account, provide it
  explicitly, for example `--downloads-per-hour 1000`.
- `--dry-run` searches and writes `download_manifest.json` without downloading.

## Output and restart behaviour

Final GeoTIFFs are stored directly in the chosen destination.
