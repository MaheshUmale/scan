# Multi-Strategy Trading Scanner System

This guide outlines the architecture, logic, and implementation steps for an enhanced stock scanning system that combines high-potential squeeze detection with high-probability trend-following pullback identification.

The goal is to resolve the conflict between "early-stage, big-move potential" (Squeezes) and "already-trending, reliable continuation" (Pullbacks) by running both strategies simultaneously and presenting them in a unified, segmented dashboard.

## 1. Context and Problem Statement

| Old Scanner Type | Goal/Issue | Resolution |
| :--- | :--- | :--- |
| **Squeeze Scanner** (`appSQZ.py`) | Find stocks with high volatility compression (potential for large moves). *Issue: Many don't fire or fail quickly.* | **High Potential Setups:** Run on lower timeframes (15m, 30m) for early alerts. |
| **General Scanner** (`scan.py`) | Basic trend/volume filtering. *Issue: Misses stocks already strongly trending or at a continuation point.* | **High Probability Setups:** Implement a dedicated **Trending/Pullback** scan on higher timeframes (60m, Daily). |

## 2. System Architecture (3 Components)

| Component | Role | Files Involved | Key Change |
| :--- | :--- | :--- | :--- |
| **Logic Layer (Python)** | Executes external market queries, runs dual-strategy filtering, scores, and saves results to MongoDB. | \`scan.py\` | **Introduces \`get_trending_pullback_query\`**. Tags results by \`Strategy_Type\`. |
| **Server Layer (Python)** | Flask server for API endpoints (\`/data\` for results, \`/settings\`). Handles background scheduling. | \`appSQZ.py\` | Minimal changes; focuses on serving the new data structure. |
| **UI/Dashboard (HTML/JS)** | Fetches combined data, separates it into two distinct display panels, and provides visual cues. | \`index.html\` | **Critical:** Displays "Squeeze Heatmap" and "Pullback Table" separately. |

## 3. Step-by-Step Implementation Guide

Follow these steps to upgrade your system.

### Step 1: Update the Scanning Logic (\`scan.py\`)

The most critical change is in \`scan.py\`. We must introduce the **Trending/Pullback** filter to capture continuation trades.

* **New Constants:** Define timeframes for each strategy type.
    * **High Potential (Squeeze):** \`HIGH_POTENTIAL_TFS = ['|15', '|30', '|60']\`
    * **High Probability (Trend):** \`HIGH_PROBABILITY_TFS = ['|60', '|240', '']\` (60m, 4h, Daily)
* **New Query Function:** Implement the \`get_trending_pullback_query\`. This query identifies stocks where:
    1.  The trend is strong (\`ADX > 25\`).
    2.  The price is above the 50-period SMA (trend direction confirmed).
    3.  The price is currently near the 20-period EMA (healthy pullback zone).
* **Consolidation:** The main scanning loop must run **both** query functions, combine the results into a single DataFrame, and add a **\`Strategy_Type\`** column (\`High_Potential\` or \`High_Probability\`).

### Step 2: Update the Server Layer (\`appSQZ.py\`)

Ensure your Flask server continues to run the scanner thread and serve the data.

* **Data Handling:** The \`/data\` endpoint must now return the combined DataFrame, including the new \`Strategy_Type\` column. The Python server logic already uses the \`scan.py\` functions to fetch and save data to MongoDB, so minimal changes are expected here, provided \`scan.py\` returns the correctly structured DataFrame.

### Step 3: Implement the Unified UI (\`index.html\`)

The dashboard must now handle the dual-strategy data and display it clearly.

* **Data Fetching:** The JavaScript fetches the single, combined dataset.
* **Client-Side Filtering:** The JavaScript logic filters the data based on the new \`Strategy_Type\` column.
    * **Squeeze Data:** \`data.filter(d => d.Strategy_Type === 'High_Potential')\`
    * **Pullback Data:** \`data.filter(d => d.Strategy_Type === 'High_Probability')\`
* **Display Segregation:**
    * **Top Panel:** Display the **Squeeze/Breakout** stocks using the existing **Heatmap** view (showing multiple timeframes).
    * **Bottom Panel:** Display the **Trending/Pullback** stocks in a clean, high-priority **Table** view. This table should explicitly show the confirmation timeframe (e.g., 60m or Daily) and the relevant MA value.

## 4. Trading Strategy Summary (How to Use the New System)

This combined view dictates two different, high-conviction trading approaches:

| Strategy | Setup Timeframe (HTF) | Entry Timeframe (LTF) | Action Required |
| :--- | :--- | :--- | :--- |
| **High Potential (Squeeze)** | 15m, 30m, 60m | 3m, 5m | **Watchlist:** Set price alerts near the breakout levels. Only enter if LTF confirms the momentum shift (e.g., strong volume spike, reversal candle). |
| **High Probability (Pullback)** | 60m, 4h, Daily | 3m, 5m | **High Priority:** These stocks are already at a strong support/entry zone. Monitor the LTF for a tight, high-volume rejection candle at the key MA (e.g., EMA 20). |

This new system ensures you capture early-stage potential *and* reliable continuation trades, providing a complete view of the market's high-quality setups.