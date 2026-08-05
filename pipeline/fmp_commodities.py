"""Canonical FMP commodity continuous-futures universe and quote units.

FMP's ``commodities-list`` mixes physical commodity futures with financial
futures and duplicate micro contracts. Silver admits only these 28 physical,
non-micro series; Bronze still preserves the complete provider list.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CommoditySpec:
    symbol: str
    name: str
    group: str
    raw_currency: str
    price_unit: str

    @property
    def price_scale(self) -> float:
        """Convert FMP's raw quote into the normalized USD price unit."""
        return 0.01 if self.raw_currency == "USX" else 1.0


COMMODITY_SPECS = (
    CommoditySpec("ZMUSD", "Soybean Meal", "agriculture", "USD", "USD/short_ton"),
    CommoditySpec("ZOUSX", "Oats", "agriculture", "USX", "USD/bushel"),
    CommoditySpec("ZLUSX", "Soybean Oil", "agriculture", "USX", "USD/pound"),
    CommoditySpec("ZCUSX", "Corn", "agriculture", "USX", "USD/bushel"),
    CommoditySpec("GCUSD", "Gold", "metal", "USD", "USD/troy_ounce"),
    CommoditySpec("ALIUSD", "Aluminum", "metal", "USD", "USD/metric_ton"),
    CommoditySpec("KEUSX", "Wheat", "agriculture", "USX", "USD/bushel"),
    CommoditySpec("HEUSX", "Lean Hogs", "livestock", "USX", "USD/pound"),
    CommoditySpec("PLUSD", "Platinum", "metal", "USD", "USD/troy_ounce"),
    CommoditySpec("HGUSD", "Copper", "metal", "USD", "USD/pound"),
    CommoditySpec("SBUSX", "Sugar", "agriculture", "USX", "USD/pound"),
    CommoditySpec("CTUSX", "Cotton", "agriculture", "USX", "USD/pound"),
    CommoditySpec("ZSUSX", "Soybeans", "agriculture", "USX", "USD/bushel"),
    CommoditySpec("LBUSD", "Lumber", "agriculture", "USD", "USD/thousand_board_feet"),
    CommoditySpec("LEUSX", "Live Cattle", "livestock", "USX", "USD/pound"),
    CommoditySpec("OJUSX", "Orange Juice", "agriculture", "USX", "USD/pound"),
    CommoditySpec("KCUSX", "Coffee", "agriculture", "USX", "USD/pound"),
    CommoditySpec("SIUSD", "Silver", "metal", "USD", "USD/troy_ounce"),
    CommoditySpec("NGUSD", "Natural Gas", "energy", "USD", "USD/MMBtu"),
    CommoditySpec("CLUSD", "Crude Oil WTI", "energy", "USD", "USD/barrel"),
    CommoditySpec("GFUSX", "Feeder Cattle", "livestock", "USX", "USD/pound"),
    CommoditySpec("ZRUSD", "Rough Rice", "agriculture", "USD", "USD/hundredweight"),
    CommoditySpec("CCUSD", "Cocoa", "agriculture", "USD", "USD/metric_ton"),
    CommoditySpec("PAUSD", "Palladium", "metal", "USD", "USD/troy_ounce"),
    CommoditySpec("BZUSD", "Brent Crude Oil", "energy", "USD", "USD/barrel"),
    CommoditySpec("DCUSD", "Class III Milk", "agriculture", "USD", "USD/hundredweight"),
    CommoditySpec("HOUSD", "Heating Oil", "energy", "USD", "USD/gallon"),
    CommoditySpec("RBUSD", "Gasoline RBOB", "energy", "USD", "USD/gallon"),
)

COMMODITY_BY_SYMBOL = {spec.symbol: spec for spec in COMMODITY_SPECS}
COMMODITY_SYMBOLS = frozenset(COMMODITY_BY_SYMBOL)

EXCLUDED_FINANCIAL_FUTURES = frozenset({
    "ESUSD", "ZQUSD", "ZBUSD", "ZFUSD", "DXUSD",
    "NQUSD", "RTYUSD", "ZTUSD", "ZNUSD", "YMUSD",
})
EXCLUDED_MICRO_DUPLICATES = frozenset({"MGCUSD", "SILUSD"})
