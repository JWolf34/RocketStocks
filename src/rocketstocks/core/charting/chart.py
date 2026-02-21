import logging
import numpy as np
import pandas as pd
import pandas_ta as ta
import mplfinance as mpf
import random as rnd
from rocketstocks.core.config.paths import validate_path

logger = logging.getLogger(__name__)

# Default path for saving plot files
PLOTS_PATH = "data/plots"


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
        self.config = {}
        self.config["last"] = kwargs.pop("last", recent_bars(self.df))
        self.config["rpad"] = kwargs.pop("rpad", 10)
        self.config["title"] = kwargs.pop("title", "Asset")
        self.config["volume"] = kwargs.pop("volume", True)

    def _validate_mpf_kwargs(self, **kwargs):
        logging.info("Validating mpf args")
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
            dfpad.iloc[:, :] = np.NaN

            df_frequency = self.df.index.value_counts().mode()[0]
            freq_delta = pd.Timedelta(df_frequency, unit=delta_unit)
            new_dr = pd.date_range(start=self.df.index[-1] + freq_delta, periods=rpad, freq=range_freq)
            dfpad.index = new_dr
            self.df = pd.concat([self.df, dfpad])

    def _plot(self, **kwargs):
        if not isinstance(self.mpfchart["plot_ratios"], tuple):
            logging.error("plot_ratios must be a tuple")
            return

        chart_title = self.config["title"]
        if "title" in kwargs and isinstance(kwargs["title"], str):
            chart_title = kwargs.pop("title")

        rpad = self.config["rpad"]
        if "rpad" in kwargs and kwargs["rpad"] > 0:
            rpad = int(kwargs["rpad"])

        def cpanel():
            return len(self.mpfchart["plot_ratios"])

        linreg = kwargs.pop("linreg", False)
        linreg_name = self.df.ta.linreg(append=True).name if linreg else ""

        midpoint = kwargs.pop("midpoint", False)
        midpoint_name = self.df.ta.midpoint(append=True).name if midpoint else ""

        ohlc4 = kwargs.pop("ohlc4", False)
        ohlc4_name = self.df.ta.ohlc4(append=True).name if ohlc4 else ""

        zscore = kwargs.pop("zscore", False)
        zscore_length = kwargs.pop("zscore_length", None)
        if isinstance(zscore_length, int) and zscore_length > 1:
            zs_name = self.df.ta.zscore(length=zscore_length, append=True).name
        elif zscore:
            zs_name = self.df.ta.zscore(append=True).name
        else:
            zs_name = ""

        clr = kwargs.pop("clr", False)
        clr_name = self.df.ta.log_return(cumulative=True, append=True).name if clr else ""

        squeeze = kwargs.pop("squeeze", False)
        lazybear = kwargs.pop("lazybear", False)
        squeeze_name = ""
        if squeeze:
            squeezes = self.df.ta.squeeze(lazybear=lazybear, detailed=True, append=True)
            squeeze_name = squeezes.name

        ama = kwargs.pop("archermas", False)
        ama_name = ""
        if ama:
            amas = self.df.ta.amat(append=True)
            ama_name = amas.name

        aobv = kwargs.pop("archerobv", False)
        aobv_name = ""
        if aobv:
            aobvs = self.df.ta.aobv(append=True)
            aobv_name = aobvs.name

        self._right_pad_df(rpad)
        mpfdf = self.df.tail(self.config["last"]).copy()
        mpfdf_columns = list(self.df.columns)

        tsig = kwargs.pop("tsignals", False)
        if tsig:
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

        taplots = []

        if linreg_name in mpfdf_columns:
            taplots += [mpf.make_addplot(mpfdf[linreg_name], type=kwargs.pop("linreg_type", "line"), color=kwargs.pop("linreg_color", "black"), linestyle="-.", width=1.2, panel=0)]

        if midpoint_name in mpfdf_columns:
            taplots += [mpf.make_addplot(mpfdf[midpoint_name], type=kwargs.pop("midpoint_type", "scatter"), color=kwargs.pop("midpoint_color", "fuchsia"), width=0.4, panel=0)]

        if ohlc4_name in mpfdf_columns:
            taplots += [mpf.make_addplot(mpfdf[ohlc4_name], ylabel=ohlc4_name, type=kwargs.pop("ohlc4_type", "scatter"), color=kwargs.pop("ohlc4_color", "blue"), alpha=0.85, width=0.4, panel=0)]

        if len(ama_name):
            amat_sr_ = mpfdf[amas.columns[-1]][mpfdf[amas.columns[-1]] > 0]
            amat_sr = amat_sr_.index.to_list()
        else:
            amat_sr = None

        sma_10_20 = kwargs.pop("sma_10_20", False)
        if sma_10_20:
            taplots += [
                mpf.make_addplot(mpfdf['SMA_10'], type="line", color="blue", width=0.8, panel=0, label="SMA_10"),
                mpf.make_addplot(mpfdf['SMA_20'], type="line", color="red", width=0.8, panel=0, label="SMA_20"),
            ]

        sma_10_50 = kwargs.pop("sma_10_50", False)
        if sma_10_50:
            taplots += [
                mpf.make_addplot(mpfdf['SMA_10'], type="line", color="blue", width=0.8, panel=0, label="SMA_10"),
                mpf.make_addplot(mpfdf['SMA_50'], type="line", color="red", width=0.8, panel=0, label="SMA_50"),
            ]

        sma_50_200 = kwargs.pop("sma_50_200", False)
        if sma_50_200:
            taplots += [
                mpf.make_addplot(mpfdf['SMA_50'], type="line", color="blue", width=0.8, panel=0, label="SMA_50"),
                mpf.make_addplot(mpfdf['SMA_200'], type="line", color="red", width=0.8, panel=0, label="SMA_200"),
            ]

        if tsig:
            taplots += [
                mpf.make_addplot(0.985 * mpfdf['Close'] * buys, type="scatter", marker="^", markersize=26, color="blue", panel=0),
                mpf.make_addplot(1.015 * mpfdf['Close'] * sells, type="scatter", marker="v", markersize=26, color="fuchsia", panel=0),
            ]

        if self.config["volume"]:
            volma = [x for x in list(self.df.columns) if x.startswith("Vol")].pop()
            max_vol = mpfdf["Volume"].max()
            ta_volume = [mpf.make_addplot(mpfdf[volma], color="blue", width=1.2, panel=1, ylim=(-.2 * max_vol, 1.5 * max_vol))]
            taplots += ta_volume

        common_plot_ratio = (3,)

        if len(aobv_name):
            _p = kwargs.pop("aobv_percenty", 0.2)
            aobv_ylim = ta_ylim(mpfdf[aobvs.columns[0]], _p)
            taplots += [
                mpf.make_addplot(mpfdf[aobvs.columns[0]], ylabel=aobv_name, color="black", width=1.5, panel=cpanel(), ylim=aobv_ylim),
                mpf.make_addplot(mpfdf[aobvs.columns[2]], color="silver", width=1, panel=cpanel(), ylim=aobv_ylim),
                mpf.make_addplot(mpfdf[aobvs.columns[3]], color="green", width=1, panel=cpanel(), ylim=aobv_ylim),
                mpf.make_addplot(mpfdf[aobvs.columns[4]], color="red", width=1.2, panel=cpanel(), ylim=aobv_ylim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        if clr_name in mpfdf_columns:
            _p = kwargs.pop("clr_percenty", 0.1)
            clr_ylim = ta_ylim(mpfdf[clr_name], _p)
            taplots += [mpf.make_addplot(mpfdf[clr_name], ylabel='CLR', color="black", width=1.5, panel=cpanel(), ylim=clr_ylim)]
            if (1 - _p) * mpfdf[clr_name].min() < 0 and (1 + _p) * mpfdf[clr_name].max() > 0:
                taplots += [mpf.make_addplot(hline(mpfdf.shape[0], 0), color="gray", width=1.2, panel=cpanel(), ylim=clr_ylim)]
            self.mpfchart["plot_ratios"] += common_plot_ratio

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
            self.mpfchart["plot_ratios"] += common_plot_ratio

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
            self.mpfchart["plot_ratios"] += common_plot_ratio

        rsi = kwargs.pop("rsi", False)
        if rsi:
            rsi_ylim = (0, 100)
            taplots += [
                mpf.make_addplot(mpfdf['RSI_14'], ylabel="Relative Strength Index", color=kwargs.pop("rsi_color", "orange"), width=1.5, panel=cpanel(), ylim=rsi_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 30), color="green", width=1, panel=cpanel(), ylim=rsi_ylim, linestyle="--"),
                mpf.make_addplot(np.full(mpfdf.shape[0], 50), color="gray", width=0.8, panel=cpanel(), ylim=rsi_ylim, linestyle="--"),
                mpf.make_addplot(np.full(mpfdf.shape[0], 70), color="red", width=1, panel=cpanel(), ylim=rsi_ylim, linestyle="--"),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        obv = kwargs.pop('obv', False)
        if obv:
            taplots += [
                mpf.make_addplot(mpfdf['OBV'], ylabel="On-Balance Volume", color=kwargs.pop("obv_color", "lightblue"), width=1.5, panel=cpanel())
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        ad = kwargs.pop('ad', False)
        if ad:
            taplots += [
                mpf.make_addplot(mpfdf['AD'], ylabel="Accumulation/\nDistribution\nIndex", color=kwargs.pop("obv_color", "lightgreen"), width=1.5, panel=cpanel())
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        macd = kwargs.pop("macd", False)
        if macd:
            macds = [x for x in mpfdf.columns if x.startswith("MACD")]
            _p = kwargs.pop("macd_percenty", 0.15)
            macd_ylim = ta_ylim(mpfdf[macds[0]], _p)
            taplots += [
                mpf.make_addplot(mpfdf[macds[0]], ylabel="Moving Average\nConvergence/Divergence", color="green", width=1.5, panel=cpanel(), label="MACD", ylim=macd_ylim),
                mpf.make_addplot(mpfdf[macds[-1]], color="orange", width=1.1, panel=cpanel(), label="Signal", ylim=macd_ylim),
                mpf.make_addplot(mpfdf[macds[1]], type="bar", alpha=0.8, color="dimgray", width=0.8, panel=cpanel(), ylim=macd_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1.2, panel=cpanel(), ylim=macd_ylim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        adx = kwargs.pop("adx", False)
        if adx:
            taplots += [
                mpf.make_addplot(mpfdf['ADX_14'], ylabel="Average Directional\nIndex", color="purple", width=1.1, panel=cpanel(), label="ADX"),
                mpf.make_addplot(mpfdf['DMP_14'], color="blue", width=1.1, panel=cpanel(), label="DI+"),
                mpf.make_addplot(mpfdf['DMN_14'], color="red", width=1.1, panel=cpanel()),
                mpf.make_addplot(hline(mpfdf.shape[0], 20), color="r", panel=cpanel(), linestyle="--"),
                mpf.make_addplot(hline(mpfdf.shape[0], 25), color="g", panel=cpanel(), linestyle="--")
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        roc = kwargs.pop("roc", False)
        if roc:
            _p = kwargs.pop("roc_percenty", 0.2)
            roc_ylim = ta_ylim(mpfdf['ROC_10'], _p)
            taplots += [
                mpf.make_addplot(mpfdf['ROC_10'], ylabel="Rate of\nChange", color="blue", width=1.1, panel=cpanel(), label="ROC", ylim=roc_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", panel=cpanel(), linestyle="--")
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        plot_returns = kwargs.pop("plot_returns", False)
        if tsig and plot_returns:
            _p = kwargs.pop("tsig_percenty", 0.23)
            treturn_ylim = ta_ylim(mpfdf["ACTRET_1"], _p)
            taplots += [
                mpf.make_addplot(mpfdf["ACTRET_1"], ylabel="Active % Return", type="bar", color="green", alpha=0.45, width=0.8, panel=cpanel(), ylim=treturn_ylim),
                mpf.make_addplot(pd.Series(mpfdf["ACTRET_1"].mean(), index=mpfdf["ACTRET_1"].index), color="blue", width=1, panel=cpanel(), ylim=treturn_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1, panel=cpanel(), ylim=treturn_ylim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

            _p = kwargs.pop("cstreturn_percenty", 0.58)
            mpfdf["CUMACTRET_1"] = mpfdf["ACTRET_1"].cumsum()
            cumactret_ylim = ta_ylim(mpfdf["CUMACTRET_1"], _p)
            taplots += [
                mpf.make_addplot(mpfdf["CUMACTRET_1"], ylabel="Cum Trend Return", type="bar", color="silver", alpha=0.45, width=1, panel=cpanel(), ylim=cumactret_ylim),
                mpf.make_addplot(0.9 * buys * mpfdf["CUMACTRET_1"], type="scatter", marker="^", markersize=14, color="green", panel=cpanel(), ylim=cumactret_ylim),
                mpf.make_addplot(1.1 * sells * mpfdf["CUMACTRET_1"], type="scatter", marker="v", markersize=14, color="red", panel=cpanel(), ylim=cumactret_ylim),
                mpf.make_addplot(hline(mpfdf.shape[0], 0), color="black", width=1, panel=cpanel(), ylim=cumactret_ylim),
            ]
            self.mpfchart["plot_ratios"] += common_plot_ratio

        if self.verbose:
            additional_ta = []
            chart_title = f"{chart_title} [{self.strategy.name}] (last {self.config['last']} bars)"
            chart_title += f"\nSince {mpfdf.index[0]} till {mpfdf.index[-1]}"
            if len(linreg_name) > 0: additional_ta.append(linreg_name)
            if len(midpoint_name) > 0: additional_ta.append(midpoint_name)
            if len(ohlc4_name) > 0: additional_ta.append(ohlc4_name)
            if len(additional_ta) > 0:
                chart_title += f"\nIncluding: {', '.join(additional_ta)}"

        if amat_sr:
            vlines_ = dict(vlines=amat_sr, alpha=0.1, colors="red")
        else:
            vlines_ = dict(vlines=mpfdf.index[0], alpha=0, colors="white")

        filename = kwargs.pop("filename", "plot")
        savepath = kwargs.pop("savepath", PLOTS_PATH)
        savefilepath = '{}/{}'.format(savepath, self.ticker)
        validate_path(savefilepath)
        savefilepath = "{}/{}.png".format(savefilepath, filename)

        save = dict(fname=savefilepath, dpi=500, pad_inches=0.25)

        logging.info("Plotting...")
        mpf.plot(mpfdf,
            title=chart_title,
            type=self.mpfchart["type"],
            style=self.mpfchart["style"],
            volume=self.config["volume"],
            figsize=self.mpfchart["figsize"],
            tight_layout=self.mpfchart["tight_layout"],
            scale_padding=self.mpfchart["scale_padding"],
            panel_ratios=self.mpfchart["plot_ratios"],
            xrotation=self.mpfchart["xrotation"],
            update_width_config=self.mpfchart["width_config"],
            show_nontrading=self.mpfchart["non_trading"],
            vlines=vlines_,
            addplot=taplots,
            savefig=save
        )
        logging.info("Plot success!")
