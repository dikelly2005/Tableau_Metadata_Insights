# Tableau Metadata Insights

This repository contains Python scripts and Snowflake SQL queries for extracting, analyzing, and managing Tableau metadata. It is designed to support data teams with auditing dashboards, tracking data lineage, and optimizing Tableau usage across an organization. This approach is currently in beta and uses CSV files saved to OneDrive, and Tableau Prep flows to load to Snowflake, but could be optimized to your unique data stack environment and privileges.

## 📁 Repository Structure

- `sql/` – Snowflake SQL scripts to query Tableau metadata from the Snowflake instance.
- `python/` – Python scripts for interacting with Tableau's Metadata API or automating metadata workflows.
- `prep/` - Tableau Prep Builder flows for loading data to Snowflake.
- `docs/` – Documentation and usage examples.
- `desktop/` - Sample Tableau workbook for viewing effective permissions

## 🚀 Getting Started

### Prerequisites

- Python 3.13+
- Tableau administrator access with Metadata API permissions
- Tableau Personal Access Token
- SharePoint or OneDrive (to act as a CSV file repository and image storage)
- A scheduler to automate running the python scripts (optional)
- Tableau Prep Conductor or equivalent to schedule the Tableau Prep flow (optional)
- Snowflake access with CRUD permissions

### Installation

Clone the repo:

```bash
git clone https://github.com/your-username/tableau-metadata-insights.git
cd tableau-metadata-insights
