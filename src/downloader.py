from src.api import find_perp, fetch_ohlcv_with_bidask, fetch_last_n_candles, fetch_ohlcv_with_bidask_aggregated


import csv
import os

def download_ohlcv_with_bidask_aggregate(
    coin='BTC/USDT',
    brokers=['binance', 'okx', 'bybit', 'bitget', 'gate']
):

    save_dir = r"D:/marketdata"

    # 如果目录不存在则创建
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"Created directory: {save_dir}")

    # ohlcv_with_bidask
    # shape = [time, o, h, l, c, v, bid_list, ask_list]
    btc_usdt_perp = find_perp(brokers, coin=coin)
    print(f"Symbols found: {btc_usdt_perp}")

    for ex, symbol in btc_usdt_perp:
        try:
            ohlcv_with_bidask = fetch_ohlcv_with_bidask_aggregated(
                ex,
                symbol,
                timeframe='3m',
                number_candles=50000
            )

            # 处理文件名
            safe_symbol = symbol.replace("/", "_").replace(":", "_")
            filename = f"{ex}_{safe_symbol}_3m.csv"
            filepath = os.path.join(save_dir, filename)

            with open(filepath, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)

                # 写入表头
                writer.writerow([
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "bid_volume_sum",
                    "ask_volume_sum",
                    "delta"
                ])

                for row in ohlcv_with_bidask:
                    ts, o, h, l, c, v, bid_v, ask_v = row

                    bid_sum = sum(bid_v)
                    ask_sum = sum(ask_v)
                    delta = ask_sum - bid_sum

                    writer.writerow([
                        ts,
                        o,
                        h,
                        l,
                        c,
                        v,
                        bid_sum,
                        ask_sum,
                        delta
                    ])

            print(f"Saved: {filepath}")

        except Exception as e:
            print(f"Exception: {e}, Exchange: {ex}")


download_ohlcv_with_bidask_aggregate(brokers=['binance'])