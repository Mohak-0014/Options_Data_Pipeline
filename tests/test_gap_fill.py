import pytest
from datetime import datetime
from config.settings import IST
from modules.aggregator.gap_fill import GapFiller
from modules.aggregator.tick_buffer import OHLCCandle

def test_gap_fill_happy_path_and_cold_start():
    filler = GapFiller()
    window_start = datetime.now(IST)
    
    # Window 1: AAPL trades, MSFT doesn't (cold start)
    candles1 = {
        "AAPL": OHLCCandle(window_start, 150.0, 155.0, 149.0, 152.0, 10)
    }
    expected = ["AAPL", "MSFT"]
    
    filled_candles1, unfillable1 = filler.fill(candles1, expected, window_start)
    
    assert len(filled_candles1) == 1
    assert "AAPL" in filled_candles1
    assert not filled_candles1["AAPL"].gap_filled
    assert "MSFT" in unfillable1  # Cold start unfillable
    
    # Window 2: MSFT trades, AAPL doesn't (gap fill AAPL)
    candles2 = {
        "MSFT": OHLCCandle(window_start, 250.0, 255.0, 249.0, 252.0, 15)
    }
    
    filled_candles2, unfillable2 = filler.fill(candles2, expected, window_start)
    
    assert len(filled_candles2) == 2
    assert "AAPL" in filled_candles2
    assert "MSFT" in filled_candles2
    assert len(unfillable2) == 0
    
    # Validate the gap-filled AAPL candle
    aapl_candle = filled_candles2["AAPL"]
    assert aapl_candle.gap_filled is True
    assert aapl_candle.open == 152.0
    assert aapl_candle.high == 152.0
    assert aapl_candle.low == 152.0
    assert aapl_candle.close == 152.0
    assert aapl_candle.tick_count == 0

def test_gap_fill_all_symbols_present():
    filler = GapFiller()
    window_start = datetime.now(IST)
    
    candles = {
        "AAPL": OHLCCandle(window_start, 150.0, 155.0, 149.0, 152.0, 10),
        "MSFT": OHLCCandle(window_start, 250.0, 255.0, 249.0, 252.0, 15)
    }
    expected = ["AAPL", "MSFT"]
    
    filled_candles, unfillable = filler.fill(candles, expected, window_start)
    
    assert len(filled_candles) == 2
    assert not filled_candles["AAPL"].gap_filled
    assert not filled_candles["MSFT"].gap_filled
    assert len(unfillable) == 0

def test_gap_fill_enabled_bypass(monkeypatch):
    import config.settings
    from main import VolatilityHarvester
    
    # Mock settings
    monkeypatch.setattr(config.settings, "GAP_FILL_ENABLED", False)
    
    assert config.settings.GAP_FILL_ENABLED is False
