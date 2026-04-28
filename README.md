# Stock Signal Web App

This project combines your existing weekly stock signal model with a private Streamlit dashboard.

The model stays mostly unchanged. The added pieces are:

- `scripts/run_weekly_model.py` — runs the model and saves dashboard-ready CSV/JSON files.
- `dashboard/app.py` — Streamlit dashboard for viewing weekly predictions and live performance.
- `.github/workflows/monday_model_run.yml` — optional GitHub Actions workflow for automatic Monday runs.
- `data/latest_predictions.csv` — latest model output used by the dashboard.
- `data/history/` — archived weekly prediction files.

## Local setup

```bash
pip install -r requirements.txt
```

## Run the model manually

Run the model across your custom universe and save the top 25 dashboard rows:

```bash
python scripts/run_weekly_model.py --universe custom --output-limit 25
```

For testing, use a smaller run:

```bash
python scripts/run_weekly_model.py --universe custom --limit 50 --output-limit 25
```

This creates:

```text
data/latest_predictions.csv
data/latest_predictions.json
data/history/YYYY-MM-DD_predictions.csv
```

## Run the dashboard locally

```bash
streamlit run dashboard/app.py
```

The dashboard compares each stock against the **Monday reference price**, not daily percent change.

It shows:

- latest model output
- current price
- change since Monday reference price
- correct/wrong so far
- accuracy by action
- accuracy by direction
- what-if portfolio returns

## What-if portfolio logic

For BUY/UP calls:

```text
position_return = change_since_monday
```

For SELL/DOWN short calls:

```text
position_return = -change_since_monday
```

The first version uses equal-weight baskets.

## GitHub Actions Monday run

The workflow file is here:

```text
.github/workflows/monday_model_run.yml
```

It runs every Monday at 16:00 UTC, which equals 12:00 PM New York during daylight saving time.

If exact noon matters during standard time, change the cron hour from `16` to `17`.

You can also run it manually from GitHub using **workflow_dispatch**.

## GitHub secrets

Do not put real API keys in the repo.

If your model uses keys, add them as GitHub repo secrets, then expose them inside the workflow `env:` block.

Example:

```yaml
env:
  GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
```

## Streamlit deployment

For a private personal webpage, the easiest path is:

1. Push this repo to GitHub as private.
2. Run the model locally or through GitHub Actions.
3. Run the dashboard locally with Streamlit.

If you later want a real hosted URL, deploy `dashboard/app.py` through Streamlit Community Cloud.


## GitHub Actions Monday Run

This project includes:

```text
.github/workflows/monday_model_run.yml
```

It runs the model every Monday around noon Eastern and saves:

```text
data/latest_predictions.csv
data/history/
```

You can also run it manually from GitHub:

```text
Actions → Monday Stock Signal Run → Run workflow
```
