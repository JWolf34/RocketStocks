import os
import pandas as pd
import stockdata as sd
import numpy as np
import pandas_ta as ta
import mplfinance as mpf
import stockdata as sd
import csv
import logging
import json
from itertools import zip_longest
import random as rnd
import strategies

# Logging configuration
logger = logging.getLogger(__name__)

# Paths for writing data
DAILY_DATA_PATH = "data/CSV/daily"
INTRADAY_DATA_PATH = "data/CSV/intraday"
FINANCIALS_PATH = "data/financials"
PLOTS_PATH = "data/plots"
ANALYSIS_PATH = "data/analysis"
ATTACHMENTS_PATH = "discord/attachments"
MINUTE_DATA_PATH = "data/CSV/minute"
UTILS_PATH = "data/utils"
SCORING_PATH = "data/scoring"

############
# Plotting #
############

def get_plot_types():
    return ['line', 'candle', 'ohlc', 'renko', 'pnf']

def get_plot_styles():
    return mpf.available_styles()

# Generate charts used for basic stock reports (those pushed to the Reports channel
# and those created by request via /run-reports or /fetch-reports)
def generate_report_charts(data, ticker):
    logger.info("Generating basic charts for ticker '{}'".format(ticker))
    if not (os.path.isdir("data/plots/" + ticker)):
            os.makedirs("data/plots/" + ticker)
    
    # Simple Moving Average 10/50
    plot_name = "Simple Moving Average 10/50"
    strategy = strategies.get_strategy(plot_name)()
    Chart(df = data, ticker = ticker, title="{} {}".format(ticker, plot_name), strategy=strategy, sma_10_50=True, volume = False, filename=strategy.short_name, tsignals=True, long_trend=strategy.signals(data))
    
    # 90-Day Candlestick
    plot_name = "90-Day Candles"
    strategy = ta.Strategy(plot_name, ta = [])
    Chart(df = data, ticker = ticker, title="{} 90-Day Candles".format(ticker), strategy=strategy, volume = True, last=recent_bars(data, "3mo"), rpad=2, filename='CANDLES')
    
    # Relative Strength Index
    plot_name = "Relative Strength Index"
    strategy = strategies.get_strategy(plot_name)()
    Chart(df = data, ticker = ticker, title = "{} {}".format(ticker, plot_name), strategy = strategy, rsi=True, volume=False, filename=strategy.short_name, tsignals=True, long_trend=strategy.signals(data))
    
    # On-Balance Volume
    plot_name = "On-Balance Volume"
    strategy = strategies.get_strategy(plot_name)()
    Chart(df = data, ticker = ticker, title = "{} {}".format(ticker, plot_name), strategy = strategy, obv=True, volume=True, last=recent_bars(data, tf="3mo"), rpad=2, filename=strategy.short_name, tsignals=True, long_trend=strategy.signals(data))
    
    # Accumulation/Distribution Index
    plot_name = "Accumulation/Distribution Index"
    strategy = strategies.get_strategy(plot_name)()
    Chart(df = data, ticker = ticker, title = "{} {}".format(ticker, plot_name), strategy = strategy, ad=True, volume=False, last=recent_bars(data, tf="3mo"), rpad=2, filename=strategy.short_name, tsignals=True, long_trend=strategy.signals(data))

    # Moving Average Convergence/Divergence
    plot_name = "Moving Average Convergence/Divergence"
    strategy = strategies.get_strategy(plot_name)()
    Chart(df = data, ticker = ticker, title = "{} {}".format(ticker, plot_name), strategy = strategy, macd=True, volume=False, filename=strategy.short_name, tsignals=True, long_trend=strategy.signals(data))

    # Average Directional Index
    plot_name = "Average Directional Index"
    strategy = strategies.get_strategy(plot_name)()
    Chart(df = data, ticker = ticker, title = "{} {}".format(ticker, plot_name), strategy = strategy, adx=True, volume=False, filename=strategy.short_name, tsignals=True, long_trend=strategy.signals(data))

    # ZScore
    plot_name = "ZScore -3/-1"
    strategy = strategies.get_strategy(plot_name)()
    Chart(df = data, ticker = ticker, title = "{} {}".format(ticker, plot_name), strategy = strategy, zscore=True, volume=False, filename=strategy.short_name, tsignals=True, long_trend=strategy.signals(data))



class Chart(object):
    def __init__(self, df: pd.DataFrame = None, ticker: str = "SPY", strategy: ta.Strategy = ta.CommonStrategy, *args, **kwargs):
        self.verbose = kwargs.pop("verbose", False)
        logging.info("Charting {} for ticker {}".format(strategy.name, ticker))

        self.ticker = ticker
        if isinstance(df, pd.DataFrame) and df.ta.datetime_ordered:
            self.df = df
            logging.debug("Loaded DataFrame {} {}".format(self.ticker, self.df.shape))
        else:
            logging.error("DataFrame missing 'ohlcv' data or index is not datetime ordered.\n")
            return None

        self._validate_chart_kwargs(**kwargs)
        self._validate_mpf_kwargs(**kwargs)
        self._validate_ta_strategy(strategy)

        # Build TA and Plot
        #logging.info("Generating TA...")
        #self.df.ta.strategy(self.strategy, verbose=self.verbose)
        logging.info("Building plot for ticker '{}'".format(self.ticker))
        self._plot(**kwargs)

    def _validate_ta_strategy(self, strategy):
        logging.info("Validating strategy to be charted")
        if strategy is not None or isinstance(strategy, ta.Strategy):
            self.strategy = strategy
        else:
            self.strategy = ta.CommonStrategy        

    def _validate_chart_kwargs(self, **kwargs):
        logging.info("Validating chart kwargs")
        """Chart Settings"""
        self.config = {}
        self.config["last"] = kwargs.pop("last", recent_bars(self.df))
        self.config["rpad"] = kwargs.pop("rpad", 10)
        self.config["title"] = kwargs.pop("title", "Asset")
        self.config["volume"] = kwargs.pop("volume", True)

    def _validate_mpf_kwargs(self, **kwargs):
        logging.info("Validating mpf args")
        # mpf global chart settings
        default_chart = mpf.available_styles()[-1]
        default_mpf_width = {
            'candle_linewidth': 0.6,
            'candle_width': 0.525,
            'volume_width': 0.525
        }
        mpfchart = {}

        mpf_style = kwargs.pop("style", "tradingview")
        if mpf_style.lower() == "random":
            mpf_styles = mpf.available_styles()
            mpfchart["style"] = mpf_styles[rnd.randrange(len(mpf_styles))]
        elif mpf_style.lower() in mpf.available_styles():
            mpfchart["style"] = mpf_style

        mpfchart["figsize"] = kwargs.pop("figsize", (12, 10))
        mpfchart["non_trading"] = kwargs.pop("nontrading", False)
        mpfchart["rc"] = kwargs.pop("rc", {'figure.facecolor': '#EDEDED'})
        mpfchart["plot_ratios"] = kwargs.pop("plot_ratios", (12, 1.7) if self.config['volume'] else (12,))
        mpfchart["scale_padding"] = kwargs.pop("scale_padding", {'left': 1, 'top': 4, 'right': 1, 'bottom': 1})
        mpfchart["tight_layout"] = kwargs.pop("tight_layout", True)
        mpfchart["type"] = kwargs.pop("type", "candle")
        mpfchart["width_config"] = kwargs.pop("width_config", default_mpf_width)
        mpfchart["xrotation"] = kwargs.pop("xrotation", 15)
        
        self.mpfchart = mpfchart


    def _right_pad_df(self, rpad: int, delta_unit: str = "D", range_freq: str = "B"):
        if rpad > 0:
            dfpad = self.df[-rpad:].copy()
            dfpad.iloc[:,:] = np.NaN

            df_frequency = self.df.index.value_counts().mode()[0] # Most common frequency
            freq_delta = pd.Timedelta(df_frequency, unit=delta_unit)
            new_dr = pd.date_range(start=self.df.index[-1] + freq_delta, periods=rpad, freq=range_freq)
            dfpad.index = new_dr # Update the padded index with new dates
            self.df = pd.concat([self.df, dfpad])
        
            
    def _plot(self, **kwargs):
        if not isinstance(self.mpfchart["plot_ratios"], tuple):
            logging.error("plot_ratios must be a tuple")
            return

        # Override Chart Title Option
        chart_title = self.config["title"]
        if "title" in kwargs and isinstance(kwargs["title"], str):
            chart_title = kwargs.pop("title")

        # Override Right Bar Padding Option
        rpad = self.config["rpad"]
        if "rpad" in kwargs and kwargs["rpad"] > 0:
            rpad = int(kwargs["rpad"])

        def cpanel():
            return len(self.mpfchart["plot_ratios"])

        # Calculate default indicators for detailed charts

        # Linear Regression
        linreg = kwargs.pop("linreg", False)
        linreg_name = self.df.ta.linreg(append=True).name if linreg else ""

        # Midpoint
        midpoint = kwargs.pop("midpoint", False)
        midpoint_name = self.df.ta.midpoint(append=True).name if midpoint else ""

        # OHLC4
        ohlc4 = kwargs.pop("ohlc4", False)
        ohlc4_name = self.df.ta.ohlc4(append=True).name if ohlc4 else ""
            
        # ZScore
        zscore = kwargs.pop("zscore", False)
        zscore_length = kwargs.pop("zscore_length", None)
        if isinstance(zscore_length, int) and zscore_length > 1:
            zs_name = self.df.ta.zscore(length=zscore_length, append=True).name
        elif zscore:
            zs_name = self.df.ta.zscore(append=True).name
        else: zs_name = ""

        # Cumulative Log Return
        clr =  kwargs.pop("clr", False)
        clr_name = self.df.ta.log_return(cumulative=True, append=True).name if clr else ""

        # Squeeze
        squeeze = kwargs.pop("squeeze", False)
        lazybear = kwargs.pop("lazybear", False)
        squeeze_name = ""
        if squeeze:
            squeezes = self.df.ta.squeeze(lazybear=lazybear, detailed=True, append=True)
            squeeze_name = squeezes.name

        # Archer Moving Averages
        ama = kwargs.pop("archermas", False)
        ama_name = ""
        if ama:
            amas = self.df.ta.amat(append=True)
            ama_name = amas.name

        # Archer OBV
        aobv = kwargs.pop("archerobv", False)
        aobv_name = ""
        if aobv:
            aobvs = self.df.ta.aobv(append=True)
            aobv_name = aobvs.name
 

        # Pad and trim Chart
        self._right_pad_df(rpad)
        mpfdf = self.df.tail(self.config["last"]).copy()
        mpfdf_columns = list(self.df.columns)
        
        tsig = kwargs.pop("tsignals", False)
        if tsig:
            # Long Trend requires Series Comparison (<=. <, = >, >=)
            # or Trade Logic that yields trends in binary.
            #default_long = mpfdf["SMA_10"] > mpfdf["SMA_20"]
            long_trend = kwargs.pop("long_trend", "")
            if not isinstance(long_trend, pd.Series):
                raise(f"[X] Must be a Series that has boolean values or values of 0s and 1s")
            mpfdf.ta.percent_return(append=True)
            mpfdf.ta.tsignals(long_trend, append=True)
            buys = np.where(mpfdf.TS_Entries > 0, 1, np.nan)
            sells = np.where(mpfdf.TS_Exits > 0, 1, np.nan)
            mpfdf["ACTRET_1"] = mpfdf.TS_Trends * mpfdf.PCTRET_1

            if all_values_are_nan(buys) or all_values_are_nan(sells):
                tsig = False
            
        # BEGIN: Custom TA Plots and Panels
        # Modify the area below 
        taplots = [] # Holds all the additional plots

        # Panel 0: Price Overlay

        # Plot default inicators if specified 

        # Linear Regression
        if linreg_name in mpfdf_columns:
            taplots += [mpf.make_addplot(mpfdf[linreg_name], type=kwargs.pop("linreg_type", "line"), color=kwargs.pop("linreg_color", "black"), linestyle="-.", width=1.2, panel=0)]

        # Midpoint
        if midpoint_name in mpfdf_columns:
            taplots += [mpf.make_addplot(mpfdf[midpoint_name], type=kwargs.pop("midpoint_type", "scatter"), color=kwargs.pop("midpoint_color", "fuchsia"), width=0.4, panel=0)]

        # OHLC4
        if ohlc4_name in mpfdf_columns:
            taplots += [mpf.make_addplot(mpfdf[ohlc4_name], ylabel=ohlc4_name, type=kwargs.pop("ohlc4_type", "scatter"), color=kwargs.pop("ohlc4_color", "blue"), alpha=0.85, width=0.4, panel=0)]

        # Archer Moving Averages
        if len(ama_name):
            amat_sr_ = mpfdf[amas.columns[-1]][mpfdf[amas.columns[-1]] > 0]
            amat_sr = amat_sr_.index.to_list()
        else:
            amat_sr = None

        # Plot Strategy indicators
 
        # SMA 10/20
        sma_10_20 = kwargs.pop("sma_10_20", False)
        if sma_10_20:
            taplots += [
                mpf.make_addplot(mpfdf['SMA_10'], type="line", color="blue", width=0.8, panel=0, label="SMA_10"),
                mpf.make_addplot(mpfdf['SMA_20'], type="line", color="red", width=0.8, panel=0, label="SMA_20"),
                
            ]
        
        # SMA 10/50
        sma_10_50 = kwargs.pop("sma_10_50", False)
        if sma_10_50:
            taplots += [
                mpf.make_addplot(mpfdf['SMA_10'], type="line", color="blue", width=0.8, panel=0, label="SMA_10"),
                mpf.make_addplot(mpfdf['SMA_50'], type="line", color="red", width=0.8, panel=0, label="SMA_50"),
                
            ]

        # SMA 50/200
        sma_50_200 = kwargs.pop("sma_50_200", False)
        if sma_50_200:
            taplots += [
                mpf.make_addplot(mpfdf['SMA_50'], type="line", color="blue", width=0.8, panel=0, label="SMA_50"),
                mpf.make_addplot(mpfdf['SMA_200'], type="line", color="red", width=0.8, panel=0, label="SMA_200"),
                
            ]
    
        # Maybe useful later? For now, eh
        """ if self.strategy.name == ta.CommonStrategy.name:
            total_sma = 0 # Check if all the overlap indicators exists before adding plots
            for c in ["SMA_10", "SMA_20", "SMA_50", "SMA_200"]:
                if c in mpfdf_columns: total_sma += 1
                else: print(f"[X] Indicator: {c} missing!")
            if total_sma == 4:
                ta_smas = [
                    mpf.make_addplot(mpfdf["SMA_10"], color="green", width=1.5, panel=0),
                    mpf.make_addplot(mpfdf["SMA_20"], color="orange", width=2, panel=0),
                    mpf.make_addplot(mpfdf["SMA_50"], color="red", width=2, panel=0),
                    mpf.make_addplot(mpfdf["SMA_200"], color="maroon", width=3, panel=0),
                ]
                taplots += ta_smas
 """
        # Buy/Sell Signals
        if tsig:
            taplots += [
                mpf.make_addplot(0.985 * mpfdf['Close'] * buys, type="scatter", marker="^", markersize=26, color="blue", panel=0),
                mpf.make_addplot(1.015 * mpfdf['Close'] * sells, type="scatter", marker="v", markersize=26, color="fuchsia", panel=0),
            ] 
                    
        # Panel 1: If volume=True, the add the VOL MA. Since we know there is only one, we immediately pop it.
        if self.config["volume"]:
            volma = [x for x in list(self.df.columns) if x.startswith("Vol")].pop()
            max_vol = mpfdf["Volume"].max()
             # Volume axis
            ta_volume = [mpf.make_addplot(mpfdf[volma], color="blue", width=1.2, panel=1, ylim=(-.2 * max_vol, 1.5 * max_vol))]
            taplots += ta_volume

        # Panels 2+
        common_plot_ratio = (3,)


        # Plot default indicators if specified 

        # Archer OBV
        if len(aobv_name):
            _p = kwargs.pop("aobv_percenty", 0.2)
            aobv_ylim = ta_ylim(mpfdf[aobvs.columns[0]], _p)
            taplots += [
                mpf.make_addplot(mpfdf[aobvs.columns[0]], ylabel=aobv_name, color="black", width=1.5, panel=cpanel(), ylim=aobv_ylim),
                mpf.make_addplot(mpfdf[aobvs.columns[2]], color="silver", width=1, panel=cpanel(), ylim=aobv_ylim),
                mpf.make_addplot(mpfdf[aobvs.columns[3]], color="green", width=1, panel=cpanel(), ylim=aobv_ylim),
                mpf.make_addplot(mpfdf[aobvs.columns[4]], color="red", width=1.2, panel=cpanel(), ylim=aobv_ylim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel """

        # Cumulative Return
        if clr_name in mpfdf_columns:
            _p = kwargs.pop("clr_percenty", 0.1)
            clr_ylim = ta_ylim(mpfdf[clr_name], _p)

            taplots += [mpf.make_addplot(mpfdf[clr_name], ylabel='CLR', color="black", width=1.5, panel=cpanel(), ylim=clr_ylim)]
            if (1 - _p) * mpfdf[clr_name].min() < 0 and (1 + _p) * mpfdf[clr_name].max() > 0:
                taplots += [mpf.make_addplot(hline(mpfdf.shape[0], 0), color="gray", width=1.2, panel=cpanel(), ylim=clr_ylim)]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel

        # ZScore
        if zs_name in mpfdf_columns:
            _p = kwargs.pop("zascore_percenty", 0.2)
            zs_ylim = ta_ylim(mpfdf[zs_name], _p)
            taplots += [
                mpf.make_addplot(mpfdf[zs_name], ylabel="ZScore", color="black", width=1.5, panel=cpanel(), ylim=zs_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], -3), color="red", width=1.2, panel=cpanel(), ylim=zs_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], -2), color="orange", width=1, panel=cpanel(), ylim=zs_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], -1), color="silver", width=1, panel=cpanel(), ylim=zs_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1.2, panel=cpanel(), ylim=zs_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 1), color="silver", width=1, panel=cpanel(), ylim=zs_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 2), color="orange", width=1, panel=cpanel(), ylim=zs_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 3), color="red", width=1.2, panel=cpanel(), ylim=zs_ylim)
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel

        # Squeeze
        if squeeze_name in mpfdf_columns:
            _p = kwargs.pop("squeeze_percenty", 0.6)
            sqz_ylim = ta_ylim(mpfdf[squeeze_name], _p)
            taplots += [
                mpf.make_addplot(mpfdf[squeezes.columns[-4]], type="bar", color="lime", alpha=0.65, width=0.8, panel=cpanel(), ylim=sqz_ylim),
                mpf.make_addplot(mpfdf[squeezes.columns[-3]], type="bar", color="green", alpha=0.65, width=0.8, panel=cpanel(), ylim=sqz_ylim),
                mpf.make_addplot(mpfdf[squeezes.columns[-2]], type="bar", color="maroon", alpha=0.65, width=0.8, panel=cpanel(), ylim=sqz_ylim),
                mpf.make_addplot(mpfdf[squeezes.columns[-1]], type="bar", color="red", alpha=0.65, width=0.8, panel=cpanel(), ylim=sqz_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1.2, panel=cpanel(), ylim=sqz_ylim),
                mpf.make_addplot(mpfdf[squeezes.columns[4]], ylabel=squeeze_name, color="green", width=2, panel=cpanel(), ylim=sqz_ylim),
                mpf.make_addplot(mpfdf[squeezes.columns[5]], color="red", width=1.8, panel=cpanel(), ylim=sqz_ylim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel """
 
        # Plot Strategy Indicators

        # RSI
        rsi = kwargs.pop("rsi", False)
        if rsi:
            rsi_ylim = (0, 100)
            taplots += [
                mpf.make_addplot(mpfdf['RSI_14'], ylabel="Relative Strength Index", color=kwargs.pop("rsi_color", "orange"), width=1.5, panel=cpanel(), ylim=rsi_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 30), color="green", width=1, panel=cpanel(), ylim=rsi_ylim, linestyle = "--"),
                mpf.make_addplot(np.full(mpfdf.shape[0], 50), color="gray", width=0.8, panel=cpanel(), ylim=rsi_ylim, linestyle = "--"),
                mpf.make_addplot(np.full(mpfdf.shape[0], 70), color="red", width=1, panel=cpanel(), ylim=rsi_ylim, linestyle = "--"),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel

        # OBV
        obv = kwargs.pop('obv', False)
        if obv:
            taplots += [
                mpf.make_addplot(mpfdf['OBV'], ylabel="On-Balance Volume", color=kwargs.pop("obv_color", "lightblue"), width=1.5, panel=cpanel())
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel

        # AD
        ad = kwargs.pop('ad', False)
        if ad:
            taplots += [
                mpf.make_addplot(mpfdf['AD'], ylabel="Accumulation/\nDistribution\nIndex", color=kwargs.pop("obv_color", "lightgreen"), width=1.5, panel=cpanel())
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel 

        # MACD
        macd = kwargs.pop("macd", False)
        if macd:
            macds = [x for x in mpfdf.columns if x.startswith("MACD")]
            _p = kwargs.pop("macd_percenty", 0.15)
            macd_ylim = ta_ylim(mpfdf[macds[0]], _p)
            taplots += [
                mpf.make_addplot(mpfdf[macds[0]], ylabel="Moving Average\nConvergence/Divergence", color="green", width=1.5, panel=cpanel(), label="MACD"),#, ylim=macd_ylim),
                mpf.make_addplot(mpfdf[macds[-1]], color="orange", width=1.1, panel=cpanel(), label="Signal"),#, ylim=macd_ylim),
                mpf.make_addplot(mpfdf[macds[1]], type="bar", alpha=0.8, color="dimgray", width=0.8, panel=cpanel()),#, ylim=macd_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1.2, panel=cpanel()),#, ylim=macd_y5lim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel 

        # ADX
        adx = kwargs.pop("adx", False)
        if adx:
            taplots += [
                mpf.make_addplot(mpfdf['ADX_14'], ylabel="Average Directional\nIndex", color="purple", width=1.1, panel=cpanel(), label="ADX"),
                mpf.make_addplot(mpfdf['DMP_14'], color="blue", width=1.1, panel=cpanel(), label="DI+"),
                mpf.make_addplot(mpfdf['DMN_14'], color="red", width=1.1, panel=cpanel()),
                mpf.make_addplot(hline(mpfdf.shape[0], 20), color="r",panel=cpanel(), linestyle="--"),
                mpf.make_addplot(hline(mpfdf.shape[0], 25), color="g",panel=cpanel(), linestyle="--")
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel 

        plot_returns = kwargs.pop("plot_returns", False)
        if tsig and plot_returns:
            _p = kwargs.pop("tsig_percenty", 0.23)
            treturn_ylim = ta_ylim(mpfdf["ACTRET_1"], _p)
            taplots += [
                mpf.make_addplot(mpfdf["ACTRET_1"], ylabel="Active % Return", type="bar", color="green", alpha=0.45, width=0.8, panel=cpanel(), ylim=treturn_ylim),
                mpf.make_addplot(pd.Series(mpfdf["ACTRET_1"].mean(), index=mpfdf["ACTRET_1"].index), color="blue", width=1, panel=cpanel(), ylim=treturn_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1, panel=cpanel(), ylim=treturn_ylim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel

            _p = kwargs.pop("cstreturn_percenty", 0.58)
            mpfdf["CUMACTRET_1"] = mpfdf["ACTRET_1"].cumsum()
            cumactret_ylim = ta_ylim(mpfdf["CUMACTRET_1"], _p)
            taplots += [
                mpf.make_addplot(mpfdf["CUMACTRET_1"], ylabel="Cum Trend Return", type="bar", color="silver", alpha=0.45, width=1, panel=cpanel(), ylim=cumactret_ylim),
                mpf.make_addplot(0.9 * buys * mpfdf["CUMACTRET_1"], type="scatter", marker="^", markersize=14, color="green", panel=cpanel(), ylim=cumactret_ylim),
                mpf.make_addplot(1.1 * sells * mpfdf["CUMACTRET_1"], type="scatter", marker="v", markersize=14, color="red", panel=cpanel(), ylim=cumactret_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1, panel=cpanel(), ylim=cumactret_ylim),
            ]            
            self.mpfchart["plot_ratios"] += common_plot_ratio # Required to add a new Panel 

        # END: Custom TA Plots and Panels
        
        if self.verbose:
            additional_ta = []
            chart_title  = f"{chart_title} [{self.strategy.name}] (last {self.config['last']} bars)"
            chart_title += f"\nSince {mpfdf.index[0]} till {mpfdf.index[-1]}"
            if len(linreg_name) > 0: additional_ta.append(linreg_name)
            if len(midpoint_name) > 0: additional_ta.append(midpoint_name)
            if len(ohlc4_name) > 0: additional_ta.append(ohlc4_name) 
            if len(additional_ta) > 0:
                chart_title += f"\nIncluding: {', '.join(additional_ta)}"

        if amat_sr:
            vlines_ = dict(vlines=amat_sr, alpha=0.1, colors="red")
        else:
            # Hidden because vlines needs valid arguments even if None 
            vlines_ = dict(vlines=mpfdf.index[0], alpha=0, colors="white")

        # Build out path to save plot to
        filename = kwargs.pop("filename", "plot")
        savepath = kwargs.pop("savepath", PLOTS_PATH)
        savefilepath = '{}/{}'.format(savepath,self.ticker)
        sd.validate_path(savefilepath)
        savefilepath = "{}/{}.png".format(savefilepath, filename)
        
        save      = dict(fname=savefilepath,dpi=500,pad_inches=0.25)

        logging.info("Plotting...")
        # Create Final Plot
        mpf.plot(mpfdf,
            title=chart_title,
            type=self.mpfchart["type"],
            style=self.mpfchart["style"],
            volume=self.config["volume"],
            figsize=self.mpfchart["figsize"],
            tight_layout=self.mpfchart["tight_layout"],
            scale_padding=self.mpfchart["scale_padding"],
            panel_ratios=self.mpfchart["plot_ratios"], # This key needs to be update above if adding more panels
            xrotation=self.mpfchart["xrotation"],
            update_width_config=self.mpfchart["width_config"],
            show_nontrading=self.mpfchart["non_trading"],
            vlines=vlines_,
            addplot=taplots,
            savefig=save
        )
        logging.info("Plot success!")

#################################
# Generate analysis for reports #
#################################

# Top-level function to generate analysis and charts on select tickers
def run_analysis(tickers):
    logger.info("Running analysis on tickers {}".format(tickers))
    for ticker in tickers:
        data = sd.fetch_daily_data(ticker)

        # Verify that data is returned
        data_size = data.size
        data_up_to_date = sd.daily_data_up_to_date(data)
        ticker_in_all_tickers = True if ticker in sd.get_all_tickers() else False
        if data_size == 0 or not data_up_to_date or not ticker_in_all_tickers:
            logger.debug("Downloading data for ticker'{}':\n Data size: {} \n Data up-to-date: {}\n Ticker in all tickers: {}".format(ticker, data_size, data_up_to_date, ticker_in_all_tickers))
            if sd.validate_ticker(ticker):
                sd.download_analyze_data(ticker)
        data = sd.fetch_daily_data(ticker)
        generate_report_charts(data, ticker)
            
###########
# Signals #
###########

def get_signals():
    logger.debug("Building signals from JSON")
    with open("{}/signals.json".format(UTILS_PATH), 'r') as signals_json:
        signals = json.load(signals_json)
        return signals

def get_signal(signal):
    logger.debug("Fetching signals for '{}'".format(signal))
    return get_signals()[signal]

def signal_rsi(close, UPPER_BOUND=70, LOWER_BOUND=30):
    logger.debug("Calculating RSI signal...")

    return ta.rsi(close) < LOWER_BOUND

def signal_macd(close):
    logger.debug("Calculating MACD signal...")
    macds = ta.macd(close)
    macd = macds[macds.columns[0]]
    macd_sig = macds[macds.columns[1]]

    return macd > macd_sig
    
def signal_sma(close, short, long):
    logger.debug("Calculating SMA signal...")

    return ta.sma(close, short) > ta.sma(close, long)

def signal_adx(close, highs, lows, TREND_UPPER=25, TREND_LOWER=20):
    logger.debug("Calculating ADX signal...")

    adxs = ta.adx(close=close, high=highs, low=lows)
    adx = adxs[adxs.columns[0]]
    dip = adxs[adxs.columns[1]]
    din = adxs[adxs.columns[2]]
    
    return (adx > TREND_UPPER) & (dip > din)

def signal_obv(close, volume):
    logger.debug("Calculating OBV signal...")

    obv = ta.obv(close=close, volume=volume)
    return  ta.increasing(ta.sma(obv, 10))

def signal_ad(high, low, close, open, volume):
    logger.debug("Calculating AD signal...")

    ad = ta.ad(high=high, low=low, close=close, volume=volume, open=open)
    return  ta.increasing(ta.sma(ad, 10))
    
def signal_zscore(close, BUY_THRESHOLD, SELL_THRESHOLD):
    
    zscore = ta.zscore(close) 
    signals = []
    for i in range(0, zscore.shape[0]):
        zscore_i = zscore.iloc[i]
        if i == 0:
            signals.append(0)
        elif zscore.iloc[i] < BUY_THRESHOLD:
            signals.append(1)
        elif zscore.iloc[i] > SELL_THRESHOLD:
            signals.append(0)
        else:
            signals.append(signals[i-1])


    return pd.Series(signals).set_axis(close.index) 

###########
# Scoring #
###########

def get_ta_signals(signals_series):
    signals = pd.DataFrame()
    signals.ta.tsignals(signals_series, append=True)
    return signals

def signals_score(data, signals):
    logger.debug("Calculating score of data from signals {}".format(signals))
    #data = sd.fetch_daily_data(ticker)
    score = 0.0
    scores_legend = {
        'BUY':1.0,
        'HOLD':0.5,
        'SELL':0.0,
        'N/A':0.0
    }

    for signal in signals:
        logger.debug("Processing signal {}".format(signal))
        params = {'data':data} | get_signal(signal)['params']
        signal_function = globals()['signal_{}'.format(get_signal(signal)['signal_func'])]
        score += scores_legend.get(signal_function(**params))

    return score

def score_eval(score, buy_threshold, sell_threshold):
    logger.debug("Evaluating score ({}) against buy threshold ({}) and sell threshold({})".format(score, buy_threshold, sell_threshold))

    if score >= buy_threshold:
        return "BUY"
    elif score <= sell_threshold:
        return "SELL"
    else:
        return "HOLD"

def generate_strategy_scores(strategy):
    logger.info("Calculating scores for strategy '{}' on masterlist tickers".format(strategy.name))
    buys = []
    holds = []
    sells = []
    tickers = sd.get_all_tickers()
     
    num_tickers = 1
    for ticker in tickers:
        logger.debug("Evaluating score for ticker '{}', {}/{}".format(ticker, num_tickers, len(tickers)))
        data = sd.fetch_daily_data(ticker)
        try:
            signals = get_ta_signals(strategy.signals(data))
            if signals.TS_Entries.iloc[-1]:
                buys.append(ticker)
            elif signals.TS_Trends.iloc[-1]:
                holds.append(ticker)
            else:
                sells.append(ticker)
        except KeyError as e:
            logger.exception("Encountered KeyError generating '{}' signal for ticker '{}':\n{}".format(strategy.name, ticker, e))
        num_tickers += 1
    
    # Validate file path
    savefilepath_root ="{}/{}/{}".format(SCORING_PATH, "strategies", strategy.short_name)
    sd.validate_path(savefilepath_root)
    savefilepath = "{}/{}_scores.csv".format(savefilepath_root, strategy.short_name)

    # Prepare data to write to CSV
    signals = zip_longest(*[buys, holds, sells], fillvalue = '')
    
    # Write scores to CSV
    with open(savefilepath, 'w', newline='') as scores:
      wr = csv.writer(scores)
      wr.writerow(("BUY", "HOLD", "SELL"))
      wr.writerows(signals)
    
    logger.debug("Scores for strategy '{}' written to '{}'".format(strategy.name, savefilepath))
    
def get_strategy_scores(strategy):
    logger.debug("Fetching scores for strategy...{}")
    scores = pd.read_csv('{}/strategies/{}/{}_scores.csv'.format(SCORING_PATH, strategy.short_name, strategy.short_name))
    return scores

def get_strategy_score_filepath(strategy):
    return '{}/strategies/{}/{}_scores.csv'.format(SCORING_PATH, strategy.short_name, strategy.short_name)


#############
# Utilities #
#############

def all_values_are_nan(values):
    if np.isnan(values).all():
        return True
    else:
        return False

# Determine if there was a crossover in the values of indicator and signal Series
# over the last 5 data point
def recent_crossover(indicator, signal):

    for i in range (1, len(indicator)):
        curr_indicator = indicator[-i]
        prev_indicator = indicator[-i-1]
        curr_signal = signal[-i]
        prev_signal = signal[-i-1]

        if prev_indicator < prev_signal and curr_indicator > curr_signal:
            return 'UP'
        elif prev_indicator > prev_signal and curr_indicator < curr_signal:
            return'DOWN'

    return None

# Function to format Millions
def format_millions(x, pos):
    "The two args are the value and tick position"
    return "%1.1fM" % (x * 1e-6)

def ctitle(indicator_name, ticker="SPY", length=100):
    return f"{ticker}: {indicator_name} from {recent_startdate} to {recent_enddate} ({length})"

# # All Data: 0, Last Four Years: 0.25, Last Two Years: 0.5, This Year: 1, Last Half Year: 2, Last Quarter: 3
# yearly_divisor = 1
# recent = int(ta.RATE["TRADING_DAYS_PER_YEAR"] / yearly_divisor) if yearly_divisor > 0 else df.shape[0]
# print(recent)
def recent_bars(df, tf: str = "1y"):
    # All Data: 0, Last Four Years: 0.25, Last Two Years: 0.5, This Year: 1, Last Half Year: 2, Last Quarter: 4
    yearly_divisor = {"all": 0, "10y": 0.1, "5y": 0.2, "4y": 0.25, "3y": 1./3, "2y": 0.5, "1y": 1, "6mo": 2, "3mo": 4}
    yd = yearly_divisor[tf] if tf in yearly_divisor.keys() else 0
    return int(ta.RATE["TRADING_DAYS_PER_YEAR"] / yd) if yd > 0 else df.shape[0]

def get_plot_timeframes():
    return {"all": 0, "10y": 0.1, "5y": 0.2, "4y": 0.25, "3y": 1./3, "2y": 0.5, "1y": 1, "6mo": 2, "3mo": 4}

def ta_ylim(series: pd.Series, percent: float = 0.1):
    smin, smax = series.min(), series.max()
    if isinstance(percent, float) and 0 <= float(percent) <= 1:
        y_min = (1 + percent) * smin if smin < 0 else (1 - percent) * smin
        y_max = (1 - percent) * smax if smax < 0 else (1 + percent) * smax
        return (y_min, y_max)
    return (smin, smax)

def hline(size, value):
    hline = np.empty(size)
    hline.fill(value)
    return hline

def test():
    
    data = sd.fetch_daily_data("GOOG")
    for strategy_name in strategies.get_strategies():
        strategy = strategies.get_strategy(strategy_name)
        signals = pd.DataFrame()
        signals.ta.tsignals(strategy.signals(strategy, data), append=True)
        print(signals)
    """ data = sd.fetch_daily_data("AGBA")
    close = data['Close']
    high = data['High']
    low = data['Low'] 
    open = data['Open']
    volume = data['Volume']

    print(signal_zscore(close=close, BUY_THRESHOLD=-3, SELL_THRESHOLD=-1)) """

    # Create trends and see their returns
    #tsignals=True,
    # Example Trends or create your own. Trend must yield Booleans
    #long_trend=ta.sma(data['Close'],10) > ta.sma(data['Close'],20), # trend: sma(close,10) > sma(close,20) [Default Example]
#     long_trend=closedf > ta.ema(closedf,5), # trend: close > ema(close,5)
#     long_trend=ta.sma(closedf,10) > ta.ema(closedf,50), # trend: sma(close,10) > ema(close,50)
#     long_trend=ta.increasing(ta.ema(closedf), 10), # trend: increasing(ema, 10)
#     long_trend=macdh > 0, # trend: macd hist > 0
#     long_trend=macd_[macd_.columns[0]] > macd_[macd_.columns[-1]], # trend: macd > macd signal
#     long_trend=ta.increasing(ta.sma(ta.rsi(closedf), 10), 5, asint=False), # trend: rising sma(rsi, 10) for the previous 5 periods
#     long_trend=ta.squeeze(highdf, lowdf, closedf, lazybear=True, detailed=True).SQZ_PINC > 0,
#     long_trend=ta.amat(closedf, 50, 200, mamode="sma").iloc[:,0], # trend: amat(50, 200) long signal using sma
    #show_nontrading=False, # Intraday use if needed
    #verbose=True, # More detail
   

if __name__ == '__main__':
    test()
    pass
    
       
