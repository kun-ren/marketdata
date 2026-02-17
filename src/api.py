import math
import time

import ccxt

def find_perp(exchanges: [str], coin='BTC/USDT') -> [tuple]:
    options_1 = {
        'enableRateLimit': True,
        'defaultType': 'future'}  # Binance
    options_2 = {
        'enableRateLimit': True,
        'defaultType': 'swap'}  # OKX / Bybit

    coin_markets = []
    for exchange in exchanges:
        exchange_obj = getattr(ccxt, exchange)(options_1)
        markets = exchange_obj.load_markets()
        has_found = False
        for s, m in markets.items():
            if m.get('type') == 'swap' and coin in s:
                has_found = True
                coin_markets.append((exchange_obj, s))
                break
        if not has_found:
            exchange_obj = getattr(ccxt, exchange)(options_2)
            markets = exchange_obj.load_markets()
            for s, m in markets.items():
                if m.get('type') == 'swap' and coin in s:
                    coin_markets.append((exchange_obj, s))
                    break

    return coin_markets


def fetch_last_n_candles(exchange: ccxt.Exchange, symbol: str, timeframe: str, number_candles: int):
    """
    Fetch the last `number_candles` OHLCV candles from now backwards in time.

    Parameters:
        exchange      : ccxt.Exchange instance
        symbol        : str, e.g., 'BTC/USDT:USDT'
        timeframe     : str, e.g., '1m', '5m'
        number_candles: int, total candles to fetch

    Returns:
        List of OHLCV candles, oldest first
    """
    candles = []
    limit = 1000 if exchange.id == 'binance' else 200  # max per request
    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    now = exchange.milliseconds()

    # calculate the timestamp of the earliest candle we want
    since = now - number_candles * timeframe_ms

    while True:
        # fetch batch
        batch_limit = min(number_candles - len(candles), limit)
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=batch_limit)

        if not batch:
            break

        # add to list
        candles.extend(batch)

        # update 'since' to last candle + 1ms
        since = batch[-1][0] + 1

        # stop if we have enough
        if len(candles) >= number_candles:
            break

        # respect rate limits
        time.sleep(exchange.rateLimit / 1000)

    # trim to exactly number_candles
    return candles[:number_candles]


def fetch_ohlcv_with_bidask(exchange, symbol, timeframe='1m', number_candles=1,):
    """
    Fetch last `number_candles` OHLCV candles with bid/ask volumes.

    Returns a list of:
    [timestamp, open, high, low, close, total_volume, bid_volume, ask_volume]
    """
    # Step 1: fetch OHLCV
    ohlcv = fetch_last_n_candles(exchange, symbol, timeframe, number_candles)
    ohlcv_with_bidask = []

    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000

    for candle in ohlcv:
        ts, o, h, l, c, v = candle
        candle_start = ts
        candle_end = ts + timeframe_ms

        # Step 2: fetch trades in this candle
        trades = []
        since = candle_start
        while True:
            batch = exchange.fetch_trades(symbol, since=since, limit=1000)
            if not batch:
                break

            # keep only trades within this candle
            batch = [t for t in batch if t['timestamp'] < candle_end]
            trades.extend(batch)

            if len(batch) < 1000:
                break

            # move since to last trade + 1ms
            since = batch[-1]['timestamp'] + 1
            time.sleep(exchange.rateLimit / 1000)

        # Step 3: aggregate bid/ask volumes
        bid_volume = sum(t['amount'] for t in trades if t['side'] == 'sell')
        ask_volume = sum(t['amount'] for t in trades if t['side'] == 'buy')

        ohlcv_with_bidask.append([ts, o, h, l, c, v, bid_volume, ask_volume])

    return ohlcv_with_bidask


def fetch_ohlcv_with_bidask_aggregated(exchange, symbol, timeframe='1m', number_candles=1, aggregation_level=5):
    """
    Fetch last `number_candles` OHLCV candles with bid/ask volumes.

    Returns a list of:
    [timestamp, open, high, low, close, total_volume, bid_volume, ask_volume]
    """
    # Step 1: fetch OHLCV
    ohlcv = fetch_last_n_candles(exchange, symbol, timeframe, number_candles)
    ohlcv_with_bidask = []

    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000

    candle_num = len(ohlcv)

    start_time = time.time()

    for index, candle in enumerate(ohlcv):
        progress = (index + 1) / candle_num
        percent = 100 * progress

        # 已耗时间
        elapsed = time.time() - start_time

        # 预估总耗时
        if progress > 0:
            estimated_total = elapsed / progress
            remaining = estimated_total - elapsed
        else:
            remaining = 0

        # 格式化为分钟:秒
        mins, secs = divmod(int(remaining), 60)
        time_str = f"{mins}m {secs}s"

        # 打印进度
        print(f"Progression: {index + 1}/{candle_num} ## {percent:.2f}% ## Estimated remaining: {time_str}")
        ts, o, h, l, c, v = candle
        candle_start = ts
        candle_end = ts + timeframe_ms

        # Step 2: fetch trades in this candle
        trades = []
        since = candle_start
        while True:
            batch = exchange.fetch_trades(symbol, since=since, limit=1000)
            if not batch:
                break

            # keep only trades within this candle
            batch = [t for t in batch if t['timestamp'] < candle_end]
            trades.extend(batch)

            if len(batch) < 1000:
                break

            # move since to last trade + 1ms
            since = batch[-1]['timestamp'] + 1
            time.sleep(exchange.rateLimit / 1000)


        # Step 3: aggregate bid/ask volumes
        highest_price = max(trades, key=lambda t: t['price'])['price']
        lowest_price = min(trades, key=lambda t: t['price'])['price']
        price_delta = highest_price - lowest_price
        interval = price_delta / aggregation_level

        # [ first_bin_max_price, second_bin_max_price, .......]
        aggregation_bins = [lowest_price + i * interval for i in range(1, aggregation_level+1)]

        sell_bins = []
        buy_bins = []
        for index, bin in enumerate(aggregation_bins):
            if index == 0:
                sell_bins.append(sum(t['amount'] for t in trades if t['side'] == 'sell' and t['price'] <= bin))
                buy_bins.append(sum(t['amount'] for t in trades if t['side'] == 'buy' and t['price'] <= bin))
            else:
                sell_bins.append(sum(t['amount'] for t in trades if t['side'] == 'sell'
                                     and aggregation_bins[index-1] < t['price'] <= bin))
                buy_bins.append(sum(t['amount'] for t in trades if t['side'] == 'buy'
                                    and aggregation_bins[index-1] < t['price'] <= bin))
        ohlcv_with_bidask.append([ts, o, h, l, c, v, sell_bins, buy_bins])

    return ohlcv_with_bidask # shape = [time, double, double, double, double, double, [aggregation_level], [aggregation_level]]


