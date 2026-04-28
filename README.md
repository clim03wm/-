# Manual Weekly Stock Signal Tracker

This is the simpler version of the dashboard.

You manually paste your model output each week. The webpage fetches prices and tracks how the predictions are doing against the Monday reference price.

## What it tracks

- Change since Monday reference price
- Correct/wrong so far for UP and DOWN calls
- Accuracy by action
- Accuracy by direction
- What-if portfolio return

## What-if logic

BUY/UP:

```text
return = change_since_monday
```

SELL/DOWN short:

```text
return = -change_since_monday
```

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
streamlit run app.py
```

## Notes

This is not connected to the prediction model. You paste the output manually.
