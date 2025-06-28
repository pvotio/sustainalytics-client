# Sustainalytics ESG Ratings Scraper

This project provides a high-throughput data pipeline for scraping and transforming company-level ESG (Environmental, Social, Governance) ratings from Sustainalytics' public website. It is designed to retrieve structured sustainability data across thousands of publicly listed companies and persist the results into a SQL Server database for analytical use.

## Overview

### Purpose

This tool is optimized for enterprise users seeking periodic access to Sustainalytics ESG risk scores for:
- Internal reporting
- Risk and exposure modeling
- ESG-aligned portfolio construction
- Sustainability compliance workflows

The scraper leverages multiprocessing and multithreading to handle large-scale data extraction in parallel, enabling timely and reliable ingestion of ESG data.

## Data Source

**Primary Endpoint:**

- `https://www.sustainalytics.com/sustapi/companyratings/getcompanyratings`

The scraper:
1. Iteratively posts to this endpoint to fetch batches of company profile URLs.
2. Visits each profile URL to extract ESG details using structured HTML parsing.

## Key Output Fields

Each record in the transformed output includes:

- `name`: Company name  
- `sustainalytics_ticker`: Unique Sustainalytics identifier  
- `ticker`: Parsed ticker symbol  
- `exchange`: Exchange suffix  
- `industry`, `country`: Entity classification  
- `desc`: Business description  
- `ftemployees`: Number of full-time employees  
- `risk_rating`: Quantitative ESG risk score  
- `risk_assessment`: Qualitative risk rating category  
- `ind_pos`, `ind_pos_total`: Industry rank and peer count  
- `uni_pos`, `uni_pos_total`: Global rank and peer count  
- `exposure_assessment`, `risk_management_assessment`: ESG component breakdown  
- `last_update`: Most recent update date (parsed)  
- `timestamp_created_utc`: Time of data ingestion

## Pipeline Architecture

1. **Ticker Discovery**  
   Posts to Sustainalytics' endpoint to discover available company profiles.

2. **Profile Scraping**  
   Executes a combination of processes and threads to fetch ESG profile pages concurrently.

3. **Data Extraction**  
   Uses `BeautifulSoup` to extract ESG ratings and company metadata from HTML elements.

4. **Transformation**  
   Normalizes and flattens nested data into a single table using `pandas`.

5. **Database Insertion**  
   Outputs data to a configured Microsoft SQL Server table via batch insertions.

## Project Structure

```
sustainalytics-client-main/
├── main.py                   # Pipeline runner
├── scraper/                  # URL discovery and HTML data scraper
│   ├── sustainalytics.py
│   ├── request.py
│   └── useragents.txt
├── transformer/              # Data transformation logic
│   └── agent.py
├── database/                 # MSSQL integration layer
│   └── mssql.py
├── config/                   # Settings and logger setup
│   ├── settings.py
│   └── logger.py
├── .env.sample               # Environment variable template
├── Dockerfile                # Containerization support
├── requirements.txt          # Python dependencies
```

## Configuration

Duplicate `.env.sample` as `.env` and populate the following:

| Variable | Description |
|----------|-------------|
| `THREAD_COUNT` | Number of threads per process |
| `LOG_LEVEL` | Log verbosity (e.g. `INFO`, `DEBUG`) |
| `OUTPUT_TABLE` | SQL Server table to store ESG data |
| `INSERTER_MAX_RETRIES` | Max DB insertion attempts |
| `REQUEST_MAX_RETRIES`, `REQUEST_BACKOFF_FACTOR` | Controls scraper retry strategy |
| `MSSQL_SERVER`, `MSSQL_DATABASE`, `MSSQL_USERNAME`, `MSSQL_PASSWORD` | SQL Server connection |
| Optional `BRIGHTDATA_*` | Proxy configuration for external IP rotation |

## Running the Scraper

### With Docker

```bash
docker build -t sustainalytics-client .
docker run --env-file .env sustainalytics-client
```

### Locally

```bash
pip install -r requirements.txt
python main.py
```

## Logging

Execution logs provide details on:
- Number of discovered tickers
- Thread/process progress
- Records fetched and inserted
- Errors and retries (if applicable)

## License

This software is provided under the MIT License. Access to Sustainalytics data is subject to their public content use policies.
