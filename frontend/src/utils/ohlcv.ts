// frontend/src/utils/ohlcv.ts

export interface OHLCV {
    timestamp: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}

export interface PriceTick {
    timestamp: number;
    price_usd: number;
    volume_sol: number; // delta from previous
}

export function aggregateOHLCV(ticks: PriceTick[], intervalMs: number): OHLCV[] {
    if (ticks.length < 1) return [];

    const candles: OHLCV[] = [];
    let currentCandle: OHLCV | null = null;
    let currentStart = Math.floor(ticks[0].timestamp / intervalMs) * intervalMs;

    for (const tick of ticks) {
        const candleStart = Math.floor(tick.timestamp / intervalMs) * intervalMs;

        if (!currentCandle || candleStart > currentStart) {
            if (currentCandle) candles.push(currentCandle);
            currentStart = candleStart;
            currentCandle = {
                timestamp: currentStart,
                open: tick.price_usd,
                high: tick.price_usd,
                low: tick.price_usd,
                close: tick.price_usd,
                volume: tick.volume_sol,
            };
        } else {
            currentCandle.high = Math.max(currentCandle.high, tick.price_usd);
            currentCandle.low = Math.min(currentCandle.low, tick.price_usd);
            currentCandle.close = tick.price_usd;
            currentCandle.volume += tick.volume_sol;
        }
    }

    if (currentCandle) candles.push(currentCandle);
    return candles;
}

export function getCandleData(ticks: PriceTick[], interval: '1m' | '5m' | '1h' | '4h' | '24h'): OHLCV[] {
    const intervals = {
        '1m': 60 * 1000,
        '5m': 5 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '24h': 24 * 60 * 60 * 1000
    };
    return aggregateOHLCV(ticks, intervals[interval]);
}
