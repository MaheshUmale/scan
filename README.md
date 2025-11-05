# Stock Breakout Screener

This is a Python-based stock screening application that identifies and displays stock breakout events in near real-time. The application is built with Flask and uses MongoDB to store data. The frontend is a responsive dashboard that provides a clear and intuitive view of breakout events.

## Features

- **Real-time Scanning**: The application continuously scans the stock market to identify breakout events as they happen.
- **Multiple Markets**: Supports both Indian (NSE) and American (NASDAQ, NYSE, AMEX) markets.
- **Potency Score**: Calculates a potency score for each breakout event to help users prioritize their analysis.
- **Interactive Dashboard**: The frontend provides an interactive and user-friendly dashboard to visualize breakout events.
- **Filtering**: Users can filter breakout events by RVOL and timeframe.
- **Manual Scanning**: In addition to automatic scanning, users can trigger a manual scan at any time.

## How It Works

The application consists of a Flask backend and a frontend built with HTML, CSS, and JavaScript. The backend is responsible for scanning the market, processing the data, and serving it to the frontend via a REST API. The frontend provides a responsive and interactive dashboard that displays the breakout events in a clear and organized manner.

### Backend

The backend is built with Flask and uses the `tradingview_screener` library to scan the market for breakout events. The scanning logic is implemented in `scan.py`, which identifies two types of breakouts:

- **Squeeze Breakouts**: These occur when the Bollinger Bands move outside of the Keltner Channels, indicating a potential increase in volatility.
- **Donchian Breakouts**: These occur when the price breaks above the upper Donchian Channel or below the lower Donchian Channel, indicating a potential trend reversal.

The backend also calculates a potency score for each breakout event based on the RVOL, timeframe, and breakout type. The data is then stored in a MongoDB database.

### Frontend

The frontend is a responsive dashboard that displays the breakout events in a clear and organized manner. It provides two views: a grid view and a treemap view. Both views allow users to filter the data by RVOL and timeframe.

## Getting Started

To get started with the application, you will need to have Python and MongoDB installed. You will also need to install the required Python libraries, which are listed in the `requirements.txt` file.

### Prerequisites

- Python 3.x
- MongoDB
- Brave Browser (for TradingView cookies)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-username/stock-breakout-screener.git
   ```
2. Install the required libraries:
   ```
   pip install -r requirements.txt
   ```
3. Start the MongoDB server:
   ```
   mongod
   ```
4. Run the application:
   ```
   python app.py --market india
   ```
   or
   ```
   python app.py --market america
   ```

## Configuration

The application can be configured using the `config.properties` file. This file contains the settings for the database, RVOL threshold, and beta threshold for each market.
