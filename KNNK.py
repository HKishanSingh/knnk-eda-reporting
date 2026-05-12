import streamlit as st
import pandas as pd
import io
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import logging
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)

# ── Prophet
PROPHET_OK    = False
PROPHET_ERROR = ""
try:
    from prophet import Prophet
    PROPHET_OK = True
except ImportError:
    PROPHET_ERROR = "Prophet not installed. Add `prophet` to requirements.txt and redeploy."
except Exception as e:
    PROPHET_ERROR = f"Prophet failed to load: {e}"

# ── XGBoost
XGB_OK    = False
XGB_ERROR = ""
try:
    from xgboost import XGBRegressor
    XGB_OK = True
except ImportError:
    XGB_ERROR = "XGBoost not installed. Add `xgboost` to requirements.txt and redeploy."
except Exception as e:
    XGB_ERROR = f"XGBoost failed to load: {e}"

# ── Safe fallback data-quality dict
EMPTY_DQ = {"n_days": 0, "is_sufficient": False, "warnings": [], "quality": "block"}

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="KNNK SmartReport Engine App",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.markdown("""
<style>
.section-title {
    font-size: 1.05rem; font-weight: 700;
    border-bottom: 2px solid currentColor;
    opacity: 0.85; padding-bottom: 0.3rem;
    margin: 1.1rem 0 0.7rem; letter-spacing: 0.01em;
}
.chip {
    display: inline-block; border: 1px solid currentColor;
    border-radius: 20px; padding: 0.15rem 0.65rem;
    font-size: 0.75rem; font-weight: 700;
    letter-spacing: 0.06em; margin-bottom: 0.4rem; opacity: 0.75;
}
div[data-testid="stDataFrame"] { width: 100% !important; }
</style>
""", unsafe_allow_html=True)

st.title("📊 KNNK SmartReport Engine App")
st.caption("GAM + DCM Reconciliation · Campaign Mapping · Insights · Prophet & XGBoost Forecast · Trend Analysis")
st.divider()

# ============================================================
# GOOGLE SHEETS
# ============================================================
SCOPE = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def get_gsheet_client():
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=SCOPE)
    except Exception:
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPE)
    return gspread.authorize(creds)

@st.cache_resource
def get_sheet(_client):
    return _client.open("Reporting with KN & NK EDA App").sheet1

try:
    _client  = get_gsheet_client()
    sheet    = get_sheet(_client)
    SHEET_OK = True
except Exception as e:
    st.sidebar.error(f"Google Sheets: {e}")
    SHEET_OK = False

# ============================================================
# MAPPING HELPERS
# ============================================================
def load_mappings():
    if not SHEET_OK:
        return {}
    try:
        result = {}
        for row in sheet.get_all_records():
            c = str(row.get("Campaign","")).strip()
            k = str(row.get("Keyword", "")).strip()
            v = str(row.get("Value",   "")).strip()
            if c and k:
                result.setdefault(c, {})[k] = v
        return result
    except Exception as e:
        st.error(f"Error loading mappings: {e}")
        return {}

def save_mappings(mappings):
    if not SHEET_OK:
        return
    try:
        sheet.clear()
        sheet.append_row(["Campaign","Keyword","Value"])
        for campaign, kv in mappings.items():
            for key, value in kv.items():
                sheet.append_row([campaign, key, value])
    except Exception as e:
        st.error(f"Error saving mappings: {e}")

def platform_key(platform, campaign):
    return f"{platform}::{campaign}"

# ============================================================
# SESSION STATE
# ============================================================
if "mappings" not in st.session_state:
    st.session_state.mappings = load_mappings()
if "show_fullview" not in st.session_state:
    st.session_state.show_fullview     = False
    st.session_state.fullview_platform = ""
    st.session_state.fullview_campaign = ""

# ============================================================
# LEGACY MAPPINGS
# ============================================================
LEGACY_MAPPINGS = {
    platform_key("GAM","Direct-NA-26-1632"): {
        "Contextual Standard":"AV",
        "Contextual Marquee":"Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Thematic Interlude":"Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Contextual Interlude":"Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Thematic Marquee":"Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Contextual YouTube":"Contextual Targeted Business Insider Video YouTube In-Stream",
        "Contextual Onsite":"Contextual Targeted Business Insider Video Business Insider On-Site Pre-Roll",
        "Audience YouTube":"Audience Targeted Business Insider Video YouTube In-Stream",
        "1P Audience Onsite":"Audience Targeted Business Insider Video Business Insider On-Site Pre-Roll",
        "3P Audience Onsite":"Audience Targeted Business Insider Video Business Insider On-Site Pre-Roll"},
    platform_key("GAM","Direct-NA-25-1625"): {
        "Contextual_YouTube_National":"Contextual Targeted Business Insider YouTube In-Stream- Geo- National",
        "Contextual_On-Site_National":"Contextual Targeted Business Insider On-Site Pre-Roll- Geo- National",
        "AV_Contextual_Banners":"AV",
        "Audience":"Audience Targeted Custom Units Marquee and Interlude Geo- National"},
}

def get_active_mappings(platform, campaign):
    pk = platform_key(platform, campaign)
    m  = st.session_state.mappings
    return m[pk] if (pk in m and m[pk]) else LEGACY_MAPPINGS.get(pk, {})

# ============================================================
# OPTIONS
# ============================================================
DEFAULT_OPTIONS = [
    "Direct-NA-26-1632","Direct-NA-25-1625"
]
dynamic_campaigns = [k.split("::",1)[1] for k in st.session_state.mappings if "::" in k]
all_options = sorted(set(DEFAULT_OPTIONS + dynamic_campaigns))

# ============================================================
# TOTAL-ROW FILTER
# ============================================================
TOTAL_KEYWORDS = ["total","grand total","subtotal","sum","overall"]

def remove_total_rows(df, col_name):
    """Safely remove summary/total rows. Handles empty DataFrames. Never raises."""
    try:
        if df.empty or col_name not in df.columns:
            return df.copy(), 0
        col_str = df[col_name].fillna("").astype(str).str.strip().str.lower()
        if col_str.empty:
            return df.copy(), 0
        mask = col_str.apply(
            lambda v: any(v == kw or v.startswith(kw) for kw in TOTAL_KEYWORDS)
        )
        return df[~mask].copy(), int(mask.sum())
    except Exception:
        return df.copy(), 0

# ============================================================
# CORE PROCESSING
# ============================================================
def process_data(df, platform, campaign, date_range=None):
    try:
        n_df = df.copy()

        # Remove Grand Total rows across all columns
        try:
            grand_mask = n_df.astype(str).apply(
                lambda col: col.str.contains("Grand Total", case=False, na=False)
            ).any(axis=1)
            n_df = n_df[~grand_mask].reset_index(drop=True)
        except Exception:
            pass  # if it fails, continue without this filter

        # Date filter — convert to string first to avoid Cloud timezone issues
        if date_range and len(date_range) == 2 and date_range[0] and date_range[1]:
            dc = next((c for c in n_df.columns if "date" in c.lower()), None)
            if dc:
                try:
                    n_df[dc] = pd.to_datetime(n_df[dc], errors="coerce")
                    # Strip timezone info if present (causes issues on Cloud pandas)
                    if hasattr(n_df[dc].dtype, "tz") and n_df[dc].dtype.tz is not None:
                        n_df[dc] = n_df[dc].dt.tz_localize(None)
                    s = pd.to_datetime(str(date_range[0]))
                    e = pd.to_datetime(str(date_range[1]))
                    n_df = n_df[(n_df[dc] >= s) & (n_df[dc] <= e)].reset_index(drop=True)
                except Exception as ex:
                    st.warning(f"⚠️ {platform}: Date filter failed — {ex}. Showing all data.")

        # Guard: nothing left after filters
        if n_df.empty:
            st.warning(
                f"⚠️ {platform}: No data remains after applying filters. "
                f"Check your date range or file contents."
            )
            return None, None

        n_df["Product"] = "Ignore"

        # Detect line-item column
        if   "Line item"         in n_df.columns: col_name = "Line item"
        elif "Package/Roadblock" in n_df.columns: col_name = "Package/Roadblock"
        elif "Placement"         in n_df.columns: col_name = "Placement"
        else:
            st.warning(
                f"⚠️ {platform}: Required column "
                f"('Line item' / 'Package/Roadblock' / 'Placement') not found."
            )
            return None, None

        n_df[col_name] = n_df[col_name].fillna("").astype(str)

        # Remove total/summary rows from the line-item column
        n_df, _ = remove_total_rows(n_df, col_name)

        if n_df.empty:
            st.warning(
                f"⚠️ {platform}: All rows identified as summary/total rows. "
                f"Check your data."
            )
            return None, None

        # Detect metric columns
        if platform == "GAM":
            imp_col   = "Ad server impressions" if "Ad server impressions" in n_df.columns else "Impressions"
            click_col = "Ad server clicks"      if "Ad server clicks"      in n_df.columns else "Clicks"
        else:
            imp_col, click_col = "Impressions", "Clicks"

        if imp_col not in n_df.columns or click_col not in n_df.columns:
            st.warning(f"⚠️ {platform}: Metric columns '{imp_col}'/'{click_col}' not found.")
            return None, None

        # Remove test rows
        before = len(n_df)
        n_df   = n_df[~n_df[col_name].str.lower().str.contains("test", na=False)].reset_index(drop=True)
        removed = before - len(n_df)
        if removed:
            st.info(f"🧹 {platform}: Removed {removed} 'test' row(s)")

        # Apply keyword mappings
        active = get_active_mappings(platform, campaign)
        n_df["_cl"] = n_df[col_name].str.lower()
        for key, value in active.items():
            kc = str(key).strip().lower()
            if kc:
                n_df.loc[n_df["_cl"].apply(lambda x: kc in x), "Product"] = value
        n_df.drop(columns=["_cl"], inplace=True)

        # Group and return
        result = n_df.groupby("Product")[[imp_col, click_col]].sum().reset_index()
        result = result.rename(columns={
            imp_col:   f"{platform}_Impressions",
            click_col: f"{platform}_Clicks"
        })
        return result, n_df

    except Exception as ex:
        st.error(f"❌ {platform}: Unexpected processing error — {ex}")
        return None, None

# ============================================================
def build_pivot(n_df, platform, key_suffix=""):
    if n_df is None or "Product" not in n_df.columns:
        return None
    numeric_cols = n_df.select_dtypes(include=["int64","float64"]).columns.tolist()
    if not numeric_cols:
        return None
    defaults = [c for c in numeric_cols if any(
        kw in c.lower() for kw in ["impression","click","view","reach"])][:4]
    selected = st.multiselect(f"Metrics — {platform}", numeric_cols,
                              default=defaults or numeric_cols[:2],
                              key=f"metrics_{platform}_{key_suffix}")
    if not selected:
        return None
    pivot = n_df.groupby("Product")[selected].sum().reset_index()
    pivot = pivot.rename(columns={
        "Ad server impressions": f"{platform}_Impressions",
        "Ad server clicks":      f"{platform}_Clicks",
        "Impressions":           f"{platform}_Impressions",
        "Clicks":                f"{platform}_Clicks"})
    ic, cc = f"{platform}_Impressions", f"{platform}_Clicks"
    if ic in pivot.columns and cc in pivot.columns:
        pivot[f"{platform}_CTR (%)"] = (
            pivot[cc] / pivot[ic].replace(0, np.nan) * 100).fillna(0).round(2)
    total = pivot.select_dtypes(include="number").sum()
    total["Product"] = "TOTAL"
    return pd.concat([pivot, pd.DataFrame([total])], ignore_index=True)

# ============================================================
# INSIGHTS ENGINE
# ============================================================
def generate_insights(df, platform=""):
    insights = []
    if df is None or df.empty:
        return insights
    gc   = df.columns[0]
    data = df[~df[gc].astype(str).str.contains("total", case=False, na=False)].copy()
    imp_c = next((c for c in data.columns if "impression" in c.lower()), None)
    clk_c = next((c for c in data.columns if "click"      in c.lower()), None)
    ctr_c = next((c for c in data.columns if "ctr"        in c.lower()), None)
    total_imp = data[imp_c].sum() if imp_c else 0
    total_clk = data[clk_c].sum() if clk_c else 0
    avg_ctr   = data[ctr_c].mean() if ctr_c else 0
    px = f"[{platform}] " if platform else ""
    insights.append(f"📊 {px}Total Impressions: {int(total_imp):,} | Total Clicks: {int(total_clk):,}")
    if ctr_c:
        emoji = "🚀" if avg_ctr>2 else ("👍" if avg_ctr>1 else "⚠️")
        label = "Strong" if avg_ctr>2 else ("Moderate" if avg_ctr>1 else "Low")
        insights.append(f"{emoji} {px}{label} avg CTR: {round(avg_ctr,2)}%")
        best  = data.sort_values(ctr_c, ascending=False).iloc[0]
        worst = data.sort_values(ctr_c, ascending=True).iloc[0]
        insights.append(f"🔥 {px}Top performer: {best[gc]} ({round(best[ctr_c],2)}%)")
        insights.append(f"📉 {px}Lowest: {worst[gc]} ({round(worst[ctr_c],2)}%)")
        low_n  = ", ".join(data[data[ctr_c]< avg_ctr][gc].astype(str).head(3))
        high_n = ", ".join(data[data[ctr_c]>=avg_ctr][gc].astype(str).head(3))
        if low_n:  insights.append(f"📉 {px}Underperformers: {low_n}")
        if high_n: insights.append(f"🌟 {px}High performers: {high_n}")
    if imp_c and total_imp > 0:
        top_row = data.sort_values(imp_c, ascending=False).iloc[0]
        insights.append(f"📈 {px}Highest impressions: {top_row[gc]}")
        share = data[imp_c] / total_imp
        if share.max() > 0.5:
            insights.append(f"⚠️ {px}Heavy dependency on '{data.loc[share.idxmax(),gc]}' (>50%)")
    rec = ("💡 Improve creatives & targeting" if avg_ctr<1
           else "💡 Optimize low performers, scale winners" if avg_ctr<2
           else "💡 Scale top performers aggressively")
    insights.append(f"{px}{rec}")
    return insights

# ============================================================
# SHARED FORECASTING CONSTANTS
# ============================================================
# Prophet thresholds (needs more data — time-series decomposition)
PROPHET_MIN_HARD = 7
PROPHET_MIN_WARN = 14
PROPHET_MIN_GOOD = 28
# XGBoost thresholds (feature engineering works with fewer rows)
XGB_MIN_HARD = 5
XGB_MIN_WARN = 10
XGB_MIN_GOOD = 21

def _make_dq(n_days, platform, model="xgb"):
    min_hard = PROPHET_MIN_HARD if model == "prophet" else XGB_MIN_HARD
    min_warn = PROPHET_MIN_WARN if model == "prophet" else XGB_MIN_WARN
    min_good = PROPHET_MIN_GOOD if model == "prophet" else XGB_MIN_GOOD
    dq = {"n_days": n_days, "warnings": [], "is_sufficient": True, "quality": "good"}
    if n_days < min_hard:
        dq.update(quality="block", is_sufficient=False)
        dq["warnings"].append(
            f"⛔ {platform}: Only **{n_days} day(s)** of data. "
            f"Need ≥ {min_hard} days for this model. Forecast blocked.")
    elif n_days < min_warn:
        dq["quality"] = "warn"
        dq["warnings"].append(
            f"⚠️ {platform}: Only **{n_days} days** of data "
            f"(need ≥ {min_warn} for reliable results). "
            f"Results will run but accuracy may be low.")
    elif n_days < min_good:
        dq["quality"] = "caution"
        dq["warnings"].append(
            f"💡 {platform}: **{n_days} days** of data. "
            f"Works — {min_good}+ days gives better pattern detection.")
    return dq

# ============================================================
# SHARED ACCURACY HELPER  (held-out validation, model-agnostic)
# ============================================================
def _mape_accuracy(y_true, y_pred):
    """
    Honest held-out MAPE accuracy.
    Returns None when data is too sparse for a reliable score
    (avoids showing 0.0% for near-zero click series).
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    valid  = y_true > 0
    # If fewer than half the holdout values are nonzero, accuracy is unreliable
    if valid.sum() < max(1, len(y_true) // 2):
        return None
    mape = (np.abs(y_true[valid] - y_pred[valid]) / y_true[valid]).mean() * 100
    return round(max(0.0, 100.0 - mape), 1)

# ============================================================
# NAIVE BASELINE  — "predict tomorrow = last known value"
# This is the simplest possible forecast. If XGBoost/Prophet
# can't beat this, the model is adding no value.
#
# WHY THIS MATTERS FOR MANAGEMENT:
# - If XGBoost accuracy > Naive accuracy → model is genuinely useful
# - If XGBoost accuracy ≈ Naive accuracy → data is too flat/short
# - If XGBoost accuracy < Naive accuracy → something is wrong
# ============================================================
def _naive_holdout_accuracy(grp, date_col, value_col, periods):
    """
    Naive forecast: predict every future day = last known value.
    Returns (accuracy_pct | None, n_holdout_days, naive_preds).
    """
    try:
        df = (grp[[date_col, value_col]].copy()
              .assign(**{date_col: lambda x: pd.to_datetime(x[date_col], errors="coerce")})
              .groupby(date_col)[value_col].sum()
              .reset_index().sort_values(date_col).reset_index(drop=True))
        n  = len(df)
        nh = min(periods, max(1, int(n * 0.20)), 7)
        nt = n - nh
        if nt < 2:
            return None, 0, np.array([])
        last_val    = float(df[value_col].iloc[nt - 1])
        naive_preds = np.full(nh, last_val)
        y_test      = df[value_col].iloc[nt:].values
        acc         = _mape_accuracy(y_test, naive_preds)
        return acc, nh, naive_preds
    except Exception:
        return None, 0, np.array([])

def _verdict(model_acc, naive_acc):
    """
    Compare model vs naive baseline. Returns (verdict_text, colour).
    colour: "green" | "orange" | "red"
    """
    if model_acc is None or naive_acc is None:
        return "Cannot compare — insufficient data", "orange"
    gap = model_acc - naive_acc
    if gap >= 5:
        return f"✅ Model beats naive by {round(gap,1)}pp — adding real value", "green"
    elif gap >= 0:
        return f"⚠️ Model marginally better than naive (+{round(gap,1)}pp) — use with caution", "orange"
    else:
        return f"❌ Model worse than naive by {round(abs(gap),1)}pp — data too short/flat to forecast reliably", "red"

# ============================================================
# XGBoost FEATURE ENGINEERING
# ============================================================
def _build_features(dates: pd.Series) -> pd.DataFrame:
    dates = pd.to_datetime(dates)
    return pd.DataFrame({
        "day_of_week":  dates.dt.dayofweek,
        "day_of_month": dates.dt.day,
        "week_of_year": dates.dt.isocalendar().week.astype(int),
        "month":        dates.dt.month,
        "day_index":    np.arange(len(dates)),
        "is_weekend":   (dates.dt.dayofweek >= 5).astype(int),
        "is_monday":    (dates.dt.dayofweek == 0).astype(int),
        "is_friday":    (dates.dt.dayofweek == 4).astype(int),
    })

def _add_lag_features(series: pd.Series, n: int) -> pd.DataFrame:
    mean_val = series.mean()
    return pd.DataFrame(
        {f"lag_{i+1}": series.shift(i+1).fillna(mean_val) for i in range(n)})

# ============================================================
# XGBoost HELD-OUT ACCURACY
# ============================================================
def _xgb_holdout_accuracy(grp, date_col, value_col, periods):
    """Returns (model_acc, naive_acc, n_holdout)."""
    try:
        df = (grp[[date_col, value_col]].copy()
              .assign(**{date_col: lambda x: pd.to_datetime(x[date_col], errors="coerce")})
              .groupby(date_col)[value_col].sum()
              .reset_index().sort_values(date_col).reset_index(drop=True))
        n = len(df)
        nh = min(periods, max(1, int(n * 0.20)), 7)
        nt = n - nh
        if nt < XGB_MIN_HARD:
            return None, None, 0
        nl = min(3, nt - 1)
        y  = df[value_col]
        X  = pd.concat([_build_features(df[date_col]), _add_lag_features(y, nl)], axis=1)
        m  = XGBRegressor(n_estimators=200, max_depth=3, learning_rate=0.05,
                          subsample=1.0, colsample_bytree=1.0,
                          random_state=42, nthread=1, verbosity=0)
        m.fit(X.iloc[:nt], y.iloc[:nt], verbose=False)
        preds     = m.predict(X.iloc[nt:])
        model_acc = _mape_accuracy(y.iloc[nt:].values, preds)
        # Naive: predict = last training value
        naive_pred = np.full(nh, float(y.iloc[nt - 1]))
        naive_acc  = _mape_accuracy(y.iloc[nt:].values, naive_pred)
        return model_acc, naive_acc, nh
    except Exception:
        return None, None, 0

# ============================================================
# XGBoost FORECAST ENGINE
# Always returns exactly 3 values: (df|None, acc|None, dq)
# ============================================================
@st.cache_data(show_spinner=False)
def forecast_xgb(n_df_json: str, product: str, periods: int, platform: str):
    def _block(reason):
        dq = dict(EMPTY_DQ); dq["warnings"] = [f"⛔ {platform}: {reason}"]
        return None, None, dq
    if not XGB_OK:
        return _block(XGB_ERROR)
    try:
        n_df = pd.read_json(io.StringIO(n_df_json))
    except Exception as ex:
        return _block(f"Could not parse data — {ex}")
    date_col = next((c for c in n_df.columns if "date" in c.lower()), None)
    if not date_col:
        return _block("No date column found.")
    try:
        n_df[date_col] = pd.to_datetime(n_df[date_col], errors="coerce")
    except Exception:
        return _block("Date column could not be parsed.")
    df = n_df[n_df["Product"] == product].copy()
    if df.empty:
        return _block(f"No rows for product '{product}'.")
    imp_col = next((c for c in df.columns if "impression" in c.lower()), None)
    clk_col = next((c for c in df.columns if "click"      in c.lower()), None)
    if not imp_col or not clk_col:
        return _block("Impression / Click columns not found.")
    grp = (df.groupby(date_col)[[imp_col, clk_col]].sum()
             .reset_index().sort_values(date_col).reset_index(drop=True))
    n_days = len(grp)
    dq = _make_dq(n_days, platform, model="xgb")
    if not dq["is_sufficient"]:
        return None, None, dq
    imp_acc, imp_naive, imp_ho = _xgb_holdout_accuracy(grp, date_col, imp_col, periods)
    clk_acc, clk_naive, clk_ho = _xgb_holdout_accuracy(grp, date_col, clk_col, periods)
    accuracy = {"Impressions": imp_acc, "Clicks": clk_acc,
                "imp_holdout_days": imp_ho, "clk_holdout_days": clk_ho,
                "imp_naive": imp_naive, "clk_naive": clk_naive}
    n_lags = min(3, n_days - 1)
    y_imp  = grp[imp_col].copy()
    y_clk  = grp[clk_col].copy()
    base_f = _build_features(grp[date_col])
    X_imp  = pd.concat([base_f, _add_lag_features(y_imp, n_lags)], axis=1)
    X_clk  = pd.concat([base_f, _add_lag_features(y_clk, n_lags)], axis=1)
    # nthread=1 → single-threaded → identical results on any CPU count
    # (multi-threaded XGBoost uses different float-op ordering per machine)
    xgb_p  = dict(n_estimators=300, max_depth=3, learning_rate=0.04,
                  subsample=1.0, colsample_bytree=1.0,
                  min_child_weight=2, gamma=0.1,
                  random_state=42, nthread=1, verbosity=0)
    try:
        m_imp = XGBRegressor(**xgb_p); m_clk = XGBRegressor(**xgb_p)
        m_imp.fit(X_imp, y_imp, verbose=False)
        m_clk.fit(X_clk, y_clk, verbose=False)
        res_imp = y_imp.values - m_imp.predict(X_imp)
        res_clk = y_clk.values - m_clk.predict(X_clk)
        last_date    = pd.to_datetime(grp[date_col].max())
        future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=periods)
        last_imp = list(y_imp.values[-n_lags:]) if n_lags > 0 else []
        last_clk = list(y_clk.values[-n_lags:]) if n_lags > 0 else []
        mean_imp = float(y_imp.mean()); mean_clk = float(y_clk.mean())
        rows_imp, rows_clk = [], []
        # Fix: set ONE global seed before the loop.
        # np.random.seed(i) inside the loop resets state each iteration,
        # which behaves differently depending on numpy version.
        rng = np.random.RandomState(42)
        for i, fd in enumerate(future_dates):
            bf = pd.DataFrame([{"day_of_week": fd.dayofweek, "day_of_month": fd.day,
                                 "week_of_year": int(fd.isocalendar().week), "month": fd.month,
                                 "day_index": n_days + i, "is_weekend": int(fd.dayofweek >= 5),
                                 "is_monday": int(fd.dayofweek == 0), "is_friday": int(fd.dayofweek == 4)}])
            lag_i = {f"lag_{l+1}": (last_imp[-(l+1)] if l < len(last_imp) else mean_imp) for l in range(n_lags)}
            lag_c = {f"lag_{l+1}": (last_clk[-(l+1)] if l < len(last_clk) else mean_clk) for l in range(n_lags)}
            pred_i = float(m_imp.predict(pd.concat([bf, pd.DataFrame([lag_i])], axis=1))[0])
            pred_c = float(m_clk.predict(pd.concat([bf, pd.DataFrame([lag_c])], axis=1))[0])
            # Use seeded rng — identical results on every machine
            boot_i = pred_i + rng.choice(res_imp, 500, replace=True)
            boot_c = pred_c + rng.choice(res_clk, 500, replace=True)
            rows_imp.append({"Date": fd.date(),
                             f"{platform}_Impressions":      max(0, round(pred_i)),
                             f"{platform}_Impressions_Low":  max(0, int(np.percentile(boot_i, 10))),
                             f"{platform}_Impressions_High": max(0, int(np.percentile(boot_i, 90)))})
            rows_clk.append({"Date": fd.date(),
                             f"{platform}_Clicks":       max(0, round(pred_c)),
                             f"{platform}_Clicks_Low":   max(0, int(np.percentile(boot_c, 10))),
                             f"{platform}_Clicks_High":  max(0, int(np.percentile(boot_c, 90)))})
            last_imp.append(pred_i); last_clk.append(pred_c)
        merged = pd.DataFrame(rows_imp).merge(pd.DataFrame(rows_clk), on="Date")
        return merged, accuracy, dq
    except Exception as ex:
        dq["warnings"].append(f"⛔ {platform}: XGBoost error — {ex}")
        return None, None, dq

# ============================================================
# Prophet HELD-OUT ACCURACY
# ============================================================
def _prophet_holdout_accuracy(grp, date_col, value_col, periods):
    """Returns (model_acc, naive_acc, n_holdout)."""
    try:
        df = (grp[[date_col, value_col]].copy()
              .assign(**{date_col: lambda x: pd.to_datetime(x[date_col], errors="coerce")})
              .groupby(date_col)[value_col].sum()
              .reset_index().sort_values(date_col)
              .rename(columns={date_col: "ds", value_col: "y"}))
        n = len(df)
        nh = min(periods, max(1, int(n * 0.20)), 7)
        nt = n - nh
        if nt < PROPHET_MIN_HARD:
            return None, None, 0
        train = df.iloc[:nt].copy()
        test  = df.iloc[nt:].copy()
        use_weekly = nt >= 14
        m = Prophet(daily_seasonality=False, weekly_seasonality=use_weekly,
                    yearly_seasonality=False, interval_width=0.80,
                    changepoint_prior_scale=0.05)
        m.fit(train)
        pred  = m.predict(m.make_future_dataframe(periods=nh))
        pred  = pred[pred["ds"].isin(test["ds"])][["ds","yhat"]]
        merged = test.merge(pred, on="ds", how="inner")
        model_acc  = _mape_accuracy(merged["y"].values, merged["yhat"].values)
        naive_pred = np.full(nh, float(train["y"].iloc[-1]))
        naive_acc  = _mape_accuracy(test["y"].values, naive_pred)
        return model_acc, naive_acc, nh
    except Exception:
        return None, None, 0

# ============================================================
# Prophet FORECAST ENGINE
# Always returns exactly 3 values: (df|None, acc|None, dq)
# ============================================================
@st.cache_data(show_spinner=False)
def forecast_prophet(n_df_json: str, product: str, periods: int, platform: str):
    def _block(reason):
        dq = dict(EMPTY_DQ); dq["warnings"] = [f"⛔ {platform}: {reason}"]
        return None, None, dq
    if not PROPHET_OK:
        return _block(PROPHET_ERROR)
    try:
        n_df = pd.read_json(io.StringIO(n_df_json))
    except Exception as ex:
        return _block(f"Could not parse data — {ex}")
    date_col = next((c for c in n_df.columns if "date" in c.lower()), None)
    if not date_col:
        return _block("No date column found.")
    try:
        n_df[date_col] = pd.to_datetime(n_df[date_col], errors="coerce")
    except Exception:
        return _block("Date column could not be parsed.")
    df = n_df[n_df["Product"] == product].copy()
    if df.empty:
        return _block(f"No rows for product '{product}'.")
    imp_col = next((c for c in df.columns if "impression" in c.lower()), None)
    clk_col = next((c for c in df.columns if "click"      in c.lower()), None)
    if not imp_col or not clk_col:
        return _block("Impression / Click columns not found.")
    grp = (df.groupby(date_col)[[imp_col, clk_col]].sum()
             .reset_index().sort_values(date_col).reset_index(drop=True))
    n_days = len(grp)
    dq = _make_dq(n_days, platform, model="prophet")
    if not dq["is_sufficient"]:
        return None, None, dq
    imp_acc, imp_naive, imp_ho = _prophet_holdout_accuracy(grp, date_col, imp_col, periods)
    clk_acc, clk_naive, clk_ho = _prophet_holdout_accuracy(grp, date_col, clk_col, periods)
    accuracy = {"Impressions": imp_acc, "Clicks": clk_acc,
                "imp_holdout_days": imp_ho, "clk_holdout_days": clk_ho,
                "imp_naive": imp_naive, "clk_naive": clk_naive}
    imp_df = grp[[date_col, imp_col]].rename(columns={date_col:"ds", imp_col:"y"})
    clk_df = grp[[date_col, clk_col]].rename(columns={date_col:"ds", clk_col:"y"})
    use_weekly = n_days >= 14
    use_daily  = n_days >= 28
    p_kw = dict(daily_seasonality=use_daily, weekly_seasonality=use_weekly,
                yearly_seasonality=False, interval_width=0.80,
                changepoint_prior_scale=0.05 if n_days < 60 else 0.15,
                uncertainty_samples=500)
    try:
        m_imp = Prophet(**p_kw); m_clk = Prophet(**p_kw)
        m_imp.fit(imp_df);       m_clk.fit(clk_df)
        fc_imp = m_imp.predict(m_imp.make_future_dataframe(periods=periods))
        fc_clk = m_clk.predict(m_clk.make_future_dataframe(periods=periods))
        # Only future rows
        last_hist = pd.to_datetime(grp[date_col].max())
        fi = fc_imp[fc_imp["ds"] > last_hist][["ds","yhat","yhat_lower","yhat_upper"]]
        fc = fc_clk[fc_clk["ds"] > last_hist][["ds","yhat","yhat_lower","yhat_upper"]]
        out_imp = fi.rename(columns={"ds":"Date",
                                     "yhat": f"{platform}_Impressions",
                                     "yhat_lower": f"{platform}_Impressions_Low",
                                     "yhat_upper": f"{platform}_Impressions_High"})
        out_clk = fc.rename(columns={"ds":"Date",
                                     "yhat": f"{platform}_Clicks",
                                     "yhat_lower": f"{platform}_Clicks_Low",
                                     "yhat_upper": f"{platform}_Clicks_High"})
        merged = out_imp.merge(out_clk, on="Date")
        merged["Date"] = pd.to_datetime(merged["Date"]).dt.date
        for c in merged.columns:
            if c != "Date":
                merged[c] = merged[c].clip(lower=0).round(0).astype(int)
        return merged, accuracy, dq
    except Exception as ex:
        dq["warnings"].append(f"⛔ {platform}: Prophet error — {ex}")
        return None, None, dq

# ============================================================
# SHARED FORECAST UI RENDERER
# Called identically from both Prophet and XGBoost sub-tabs.
# ============================================================
def render_forecast_ui(model_label, forecast_fn, model_key,
                       gam_clean, dcm_clean, option):
    """
    model_label : "Prophet" | "XGBoost"
    forecast_fn : forecast_prophet | forecast_xgb
    model_key   : unique string for widget keys ("prophet" | "xgb")
    """
    has_gam = gam_clean is not None and "Product" in gam_clean.columns
    has_dcm = dcm_clean is not None and "Product" in dcm_clean.columns
    def has_date(df): return df is not None and any("date" in c.lower() for c in df.columns)

    if not has_gam and not has_dcm:
        st.info("Upload at least one file (GAM or DCM) in the **Reporting** tab first.")
        return
    if not has_date(gam_clean) and not has_date(dcm_clean):
        st.warning("⚠️ No date column found. Forecasting requires a date column.")
        return

    all_products = set()
    if has_gam:
        all_products.update(gam_clean[gam_clean["Product"]!="Ignore"]["Product"].dropna().unique())
    if has_dcm:
        all_products.update(dcm_clean[dcm_clean["Product"]!="Ignore"]["Product"].dropna().unique())
    all_products = sorted(all_products)

    if not all_products:
        st.warning("⚠️ No mapped products found. Check your mappings.")
        return

    st.markdown('<div class="section-title">Settings</div>', unsafe_allow_html=True)
    fc1, fc2, fc3 = st.columns([3,1,1])
    with fc1:
        sel_product = st.selectbox("Select Product", all_products,
                                   key=f"product_{model_key}",
                                   help="Forecasts run independently per platform.")
    with fc2:
        forecast_days = st.selectbox("Forecast Days", [7,14,21,30],
                                     key=f"days_{model_key}",
                                     help="7–14 days is most reliable.")
    with fc3:
        st.markdown("<br>", unsafe_allow_html=True)
        run_btn = st.button(f"🚀 Run {model_label}", use_container_width=True,
                            key=f"run_{model_key}")

    st.caption("📌 Forecast accuracy is highest for 7–14 day horizons based on historical patterns.")
    st.caption("📌 Accuracy uses held-out validation — honest out-of-sample, not training-data fit.")

    if not run_btn:
        return

    gam_fc, gam_acc, gam_dq = None, None, dict(EMPTY_DQ)
    dcm_fc, dcm_acc, dcm_dq = None, None, dict(EMPTY_DQ)

    with st.spinner(f"Running {model_label} forecast… please wait."):
        if has_gam and has_date(gam_clean):
            gam_fc, gam_acc, gam_dq = forecast_fn(
                gam_clean.to_json(), sel_product, forecast_days, "GAM")
        if has_dcm and has_date(dcm_clean):
            dcm_fc, dcm_acc, dcm_dq = forecast_fn(
                dcm_clean.to_json(), sel_product, forecast_days, "DCM")

    # Data quality panel
    st.markdown('<div class="section-title">📋 Data Quality Check</div>', unsafe_allow_html=True)
    dq_c1, dq_c2 = st.columns(2)
    for dq, plat, col in [(gam_dq,"GAM",dq_c1),(dcm_dq,"DCM",dq_c2)]:
        with col:
            quality = dq.get("quality","block")
            st.metric(f"{plat} — Historical Days", f"{dq.get('n_days',0)} days",
                      delta=("Sufficient ✅"       if quality in ("good","caution") else
                             "Use with caution ⚠️" if quality == "warn" else "Insufficient ⛔"),
                      delta_color="normal" if quality=="good" else "off")
            for w in dq.get("warnings",[]):
                if   w.startswith("⛔"): st.error(w)
                elif w.startswith("⚠️"): st.warning(w)
                else:                     st.info(w)

    if gam_fc is None and dcm_fc is None:
        st.error("❌ Forecast could not run. See data quality check above.")
        return

    # Merge results
    if gam_fc is not None and dcm_fc is not None:
        final = gam_fc.merge(dcm_fc, on="Date", how="outer")
    elif gam_fc is not None:
        final = gam_fc
    else:
        final = dcm_fc

    final = final.sort_values("Date").reset_index(drop=True)
    final["Date"] = pd.to_datetime(final["Date"]).dt.date

    st.success(f"✅ {model_label} forecast — {forecast_days} day horizon for **{sel_product}**")

    # ── Accuracy cards + model vs naive comparison
    st.markdown('<div class="section-title">🎯 Model Accuracy vs Naive Baseline (Held-Out Validation)</div>',
                unsafe_allow_html=True)
    st.caption(
        "**How to read:** Last ~20% of actual dates hidden from training, predicted, then compared to reality. "
        "Naive baseline = 'predict tomorrow = today's value' — the simplest possible forecast. "
        "If the model can't beat naive, the data is too short or flat to forecast reliably."
    )

    # Build comparison rows for table
    comparison_rows = []
    acc_cols = st.columns(4); col_idx = 0
    for plat, acc in [("GAM",gam_acc),("DCM",dcm_acc)]:
        if not acc: continue
        for metric, ho_key, naive_key in [
            ("Impressions","imp_holdout_days","imp_naive"),
            ("Clicks","clk_holdout_days","clk_naive")
        ]:
            val       = acc.get(metric)
            naive_val = acc.get(naive_key)
            ho_n      = acc.get(ho_key, 0)
            if col_idx < 4:
                if val is None:
                    acc_cols[col_idx].metric(f"{plat} {metric} Accuracy","N/A",
                                             delta="Sparse/zero data",delta_color="off")
                else:
                    fit = "Good ✅" if val>=80 else ("Fair ⚠️" if val>=60 else "Low ❌")
                    acc_cols[col_idx].metric(
                        f"{plat} {metric} Accuracy", f"{val}%",
                        delta=f"{fit} · Naive: {naive_val}% · {ho_n}d hold-out" if naive_val else f"{fit} · {ho_n}d hold-out",
                        delta_color="normal" if val>=80 else "off")
                col_idx += 1
            # Collect for comparison table
            verdict_text, colour = _verdict(val, naive_val)
            comparison_rows.append({
                "Platform":          plat,
                "Metric":            metric,
                "Model Accuracy":    f"{val}%" if val is not None else "N/A",
                "Naive Accuracy":    f"{naive_val}%" if naive_val is not None else "N/A",
                "Holdout Days":      ho_n,
                "Verdict":           verdict_text,
            })

    if col_idx == 0:
        st.info("Accuracy not available — data may be too sparse.")

    # ── Model vs Naive comparison table
    if comparison_rows:
        st.markdown('<div class="section-title">📊 Model vs Naive Baseline — Full Comparison</div>',
                    unsafe_allow_html=True)
        st.caption(
            "This table answers: **Is the model actually adding value over a simple guess?** "
            "A model that beats naive is genuinely learning your campaign patterns. "
            "One that doesn't means more data is needed before forecasts are trustworthy."
        )

        cmp_df = pd.DataFrame(comparison_rows)
        st.dataframe(cmp_df, use_container_width=True, hide_index=True)

        # Overall verdict — use impressions as primary metric
        imp_rows = [r for r in comparison_rows if r["Metric"] == "Impressions"]
        verdicts = [_verdict(
            float(r["Model Accuracy"].replace("%","")) if r["Model Accuracy"] != "N/A" else None,
            float(r["Naive Accuracy"].replace("%","")) if r["Naive Accuracy"] != "N/A" else None
        ) for r in imp_rows]

        if verdicts:
            st.markdown("**Overall Impression Forecast Trustworthiness:**")
            for r, (vt, vc) in zip(imp_rows, verdicts):
                if   vc == "green":  st.success(f"{r['Platform']}: {vt}")
                elif vc == "orange": st.warning(f"{r['Platform']}: {vt}")
                else:                st.error(f"{r['Platform']}: {vt}")

        with st.expander("📖 How to explain and understand the results"):
            st.markdown("""
### What is a Naive Baseline?
The simplest possible forecast: "whatever happened today will happen tomorrow."
No model, no learning, just repeat the last known value.

### Why do we compare against it?
If our model (XGBoost or Prophet) can't predict better than this simple rule,
it means our historical data doesn't have enough pattern for machine learning to learn from.
This is an honest quality check — not a failure of the tool, but a signal about the data.

### How to read the verdict

| Verdict | Meaning | Action |
|---|---|---|
| ✅ Model beats naive | The model is learning real patterns (e.g. weekday spikes) | Use forecast with confidence |
| ⚠️ Marginal improvement | Some pattern detected but weak | Use as directional guide only |
| ❌ Model worse than naive | Data too short, too flat, or too noisy | Collect more data before forecasting |

### What if Clicks show N/A?
Click volumes are often very low (0–5 per day). A series that's mostly zeros
can't be reliably forecast by any model. This is a data characteristic, not a bug.
Focus on Impressions accuracy for campaign planning.

### The key number for management
If you see "Model beats naive by 10pp+" on Impressions → the forecast is
genuinely informed by your campaign's historical delivery patterns and is
worth presenting to clients.
""")

    # ── Auto confidence label based on data size + model vs naive
    def _confidence_label(n_days, model_acc, naive_acc, forecast_days):
        """
        Compute an overall confidence label for the forecast.
        Returns (label, level) where level is "high"|"medium"|"low"|"caution"
        """
        # Data size check
        if n_days is None or n_days < 10:
            return ("🔴 Low Confidence — Less than 10 days of data. "
                    "Forecast is extrapolation only, not pattern-based.", "low")
        if n_days < 21:
            size_level = "medium"
            size_note  = f"only {n_days} days of history"
        else:
            size_level = "high"
            size_note  = f"{n_days} days of history"

        # Horizon check — accuracy degrades with longer forecasts
        if forecast_days > 14:
            horizon_note = f"{forecast_days}-day horizon (reliability decreases beyond 14 days)"
        else:
            horizon_note = f"{forecast_days}-day horizon (reliable range)"

        # Model vs naive
        if model_acc is None or naive_acc is None:
            return (f"🟡 Medium Confidence — {size_note}, {horizon_note}. "
                    f"Click accuracy unavailable (sparse data).", "medium")

        gap = model_acc - naive_acc
        if gap >= 5 and size_level == "high" and forecast_days <= 14:
            return (f"🟢 High Confidence — Model beats naive by {round(gap,1)}pp on impressions. "
                    f"{size_note}, {horizon_note}. Results are suitable for client reporting.", "high")
        elif gap >= 5:
            return (f"🟡 Medium-High Confidence — Model beats naive by {round(gap,1)}pp. "
                    f"{size_note}, {horizon_note}. Use as directional guide.", "medium")
        elif gap >= 0:
            return (f"🟡 Medium Confidence — Marginal improvement over naive ({round(gap,1)}pp). "
                    f"{size_note}, {horizon_note}. Use for planning only, not commitments.", "medium")
        else:
            return (f"🔴 Low Confidence — Model does not beat naive baseline. "
                    f"{size_note}, {horizon_note}. More data needed before trusting this forecast.", "low")

    # Compute confidence for GAM (primary) and DCM
    st.markdown('<div class="section-title">🏷️ Forecast Confidence Level</div>', unsafe_allow_html=True)
    st.caption("Auto-computed from: data size + model accuracy + horizon length + model vs naive comparison.")

    conf_c1, conf_c2 = st.columns(2)
    for plat, acc, dq_info, col in [
        ("GAM", gam_acc, gam_dq, conf_c1),
        ("DCM", dcm_acc, dcm_dq, conf_c2)
    ]:
        if not acc:
            with col:
                st.info(f"**{plat}:** No data processed — confidence N/A")
            continue
        n_d  = dq_info.get("n_days", 0)
        m_a  = acc.get("Impressions")
        n_a  = acc.get("imp_naive")
        label, level = _confidence_label(n_d, m_a, n_a, forecast_days)
        with col:
            st.markdown(f"**{plat} Impressions Forecast**")
            if   level == "high":    st.success(label)
            elif level == "medium":  st.warning(label)
            else:                    st.error(label)

    # ── Honest limitations box
    with st.expander("⚠️ Known limitations of this forecast — read before sharing with clients"):
        st.markdown(f"""
### What this forecast can and cannot do

**✅ What it does well:**
- Learns weekday delivery patterns (e.g. Monday spikes, weekend dips)
- Captures campaign ramp-up / wind-down trends
- Gives honest accuracy via held-out validation (not fake 100%)
- Compares against naive baseline to prove it adds value
- Provides confidence intervals (Low / High bands)

**⚠️ Known limitations:**

| Factor | Impact on accuracy |
|---|---|
| Campaign data < 21 days | High — model has little pattern to learn from |
| Forecast horizon > 14 days | Medium — errors compound day-over-day |
| Clicks data (sparse/near-zero) | High — click forecasts are unreliable by nature |
| Budget pacing / flight end dates | Medium — model doesn't know if budget runs out |
| External events (holidays, news) | Medium — not captured unless in historical data |
| Current forecast horizon | **{forecast_days} days** {"✅ within reliable range" if forecast_days <= 14 else "⚠️ beyond recommended 14 days"} |

**📋 Recommended use:**
- **7–14 day forecasts with 21+ days of data** → suitable for client reporting with confidence label shown
- **< 14 days of data** → internal planning only, not client-facing
- **Click forecasts** → always treat as directional, never as precise commitments
- **Impressions forecasts beating naive by 5pp+** → reliable enough for delivery estimates

**💬 What to tell management / clients:**
> *"Our forecast uses XGBoost/Prophet trained on {dq_info.get('n_days', 'X') if 'gam_dq' not in dir() else gam_dq.get('n_days','X')} days of actual campaign delivery data.
> The model accuracy is measured by hiding real data from the model and testing its predictions — not by comparing against training data.
> Impressions forecasts are reliable for planning. Click forecasts are directional only due to low daily volumes."*
""")

    # Summary metrics
    future_only = final.tail(forecast_days)
    st.markdown('<div class="section-title">Forecast Summary — Next Period</div>', unsafe_allow_html=True)
    s_cols = st.columns(4); sidx = 0
    for plat in ["GAM","DCM"]:
        ic, cc = f"{plat}_Impressions", f"{plat}_Clicks"
        if ic in future_only.columns and sidx < 4:
            s_cols[sidx].metric(f"{plat} Impressions (next {forecast_days}d)",
                                f"{int(future_only[ic].sum()):,}"); sidx += 1
        if cc in future_only.columns and sidx < 4:
            s_cols[sidx].metric(f"{plat} Clicks (next {forecast_days}d)",
                                f"{int(future_only[cc].sum()):,}"); sidx += 1

    # Table
    st.markdown('<div class="section-title">Full Forecast Table</div>', unsafe_allow_html=True)
    st.caption("Date = calendar day · Predicted = best estimate · "
               "_Low = pessimistic · _High = optimistic · 80% CI: ~4 out of 5 days.")
    st.dataframe(final, use_container_width=True)

    # Charts
    imp_cols = [c for c in final.columns if "impression" in c.lower()
                and "low" not in c.lower() and "high" not in c.lower()]
    if imp_cols:
        st.markdown('<div class="section-title">📈 Impression Forecast Trend</div>', unsafe_allow_html=True)
        st.line_chart(final.set_index("Date")[imp_cols])

    clk_cols = [c for c in final.columns if "click" in c.lower()
                and "low" not in c.lower() and "high" not in c.lower()]
    if clk_cols:
        st.markdown('<div class="section-title">🖱️ Click Forecast Trend</div>', unsafe_allow_html=True)
        st.line_chart(final.set_index("Date")[clk_cols])

    # Download
    st.markdown('<div class="section-title">Download</div>', unsafe_allow_html=True)
    buf = io.BytesIO(); final.to_excel(buf, index=False); buf.seek(0)
    st.download_button(f"⬇️ Download {model_label} Forecast Excel", data=buf,
                       file_name=f"{option}_{sel_product}_{model_key}_{forecast_days}d.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       use_container_width=True, key=f"dl_{model_key}")
    st.caption("⚠️ Predictions are directional estimates. "
               "Accuracy % = held-out validation, not guaranteed future performance.")

# ============================================================
# FILE READER
# ============================================================
def read_uploaded_file(uploaded, key_prefix):
    if uploaded is None:
        return None
    if uploaded.name.endswith(".csv"):
        return pd.read_csv(uploaded)
    xls     = pd.ExcelFile(uploaded)
    default = "Ad Manager Report" if "Ad Manager Report" in xls.sheet_names else xls.sheet_names[0]
    chosen  = st.selectbox(f"Select sheet — {uploaded.name}", xls.sheet_names,
                           index=xls.sheet_names.index(default),
                           key=f"{key_prefix}_sheet")
    return pd.read_excel(uploaded, sheet_name=chosen)

# ============================================================
# MAPPING MANAGER UI
# ============================================================
def mapping_manager_ui(platform, _all_options):
    mappings = st.session_state.mappings
    st.markdown(f"##### {platform} Mapping")
    with st.expander(f"➕ Add / Update {platform} Mapping"):
        camp_id = st.text_input("Campaign ID", key=f"{platform}_add_id")
        pk = platform_key(platform, camp_id) if camp_id else ""
        if camp_id and pk in mappings:
            st.caption("Existing:"); st.json(mappings[pk])
        clear_old = st.checkbox("Replace existing", key=f"{platform}_clear")
        bulk_text = st.text_area("keyword = value  (one per line)", height=130,
                                 placeholder="Audience = Audience Data\nAV = Added Value",
                                 key=f"{platform}_bulk")
        if st.button(f"💾 Save {platform} Mapping", use_container_width=True, key=f"{platform}_save"):
            if not camp_id.strip():
                st.error("Enter Campaign ID")
            else:
                mappings[pk] = {} if clear_old else mappings.get(pk, {})
                added = skipped = 0
                for line in bulk_text.split("\n"):
                    line = line.strip()
                    if not line: continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip()
                        if k and v: mappings[pk][k] = v; added += 1
                        else: skipped += 1
                    else: skipped += 1
                save_mappings(mappings)
                st.session_state.mappings = mappings
                st.success(f"✅ {added} mapping(s) saved")
                if skipped: st.warning(f"⚠️ {skipped} line(s) skipped")
                st.rerun()
    with st.expander(f"👁️ View {platform} Mapping"):
        pkeys = [k for k in mappings if k.startswith(f"{platform}::")]
        if pkeys:
            labels = [k.split("::",1)[1] for k in pkeys]
            chosen_label = st.selectbox("Campaign", labels, key=f"{platform}_view_sel")
            chosen_pk    = platform_key(platform, chosen_label)
            if chosen_pk in mappings:
                mdf = pd.DataFrame(list(mappings[chosen_pk].items()),
                                   columns=["Keyword","Mapped Value"])
                st.dataframe(mdf, use_container_width=True, height=180)
                if st.button(f"⛶ Full View — {chosen_label}", use_container_width=True,
                             key=f"{platform}_fullview_btn"):
                    st.session_state.show_fullview     = True
                    st.session_state.fullview_platform = platform
                    st.session_state.fullview_campaign = chosen_label
                    st.rerun()
        else:
            st.info(f"No {platform} mappings saved yet.")
    with st.expander(f"🗑️ Delete {platform} Campaign"):
        pkeys = [k for k in mappings if k.startswith(f"{platform}::")]
        if pkeys:
            del_labels = [k.split("::",1)[1] for k in pkeys]
            del_label  = st.selectbox("Campaign to delete", del_labels, key=f"{platform}_del_sel")
            if st.button(f"❌ Delete {del_label}", use_container_width=True, key=f"{platform}_del_btn"):
                del mappings[platform_key(platform, del_label)]
                save_mappings(mappings); st.session_state.mappings = mappings
                st.success("Deleted ✅"); st.rerun()
        else:
            st.info(f"No {platform} campaigns to delete.")

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    if st.button("🔄 Refresh Mappings", use_container_width=True):
        st.session_state.mappings = load_mappings()
        st.toast("Mappings refreshed ✅"); st.rerun()
    st.divider(); st.header("⚙️ Configuration")
    option = st.selectbox("Campaign / Report", all_options)
    st.divider(); st.subheader("📅 Date Filter")
    apply_date = st.checkbox("Enable Date Filter")
    date_range = None
    if apply_date:
        c1, c2 = st.columns(2)
        with c1: sd = st.date_input("Start", key="date_start")
        with c2: ed = st.date_input("End",   key="date_end")
        if sd and ed:
            if sd <= ed:
                date_range = (str(sd), str(ed))
            else:
                st.warning("⚠️ Start date must be before or equal to End date.")
    st.divider(); st.subheader("🧮 CPM Calculator")
    budget = st.number_input("Budget ($)", min_value=0.0, step=100.0)
    impressions = st.number_input("Impressions", min_value=0.0, step=1000.0)
    cpm_in = st.number_input("CPM ($)", min_value=0.0, step=0.1)
    if budget > 0 and impressions > 0:
        st.success(f"CPM = ${round((budget/impressions)*1000,2)}")
    elif impressions > 0 and cpm_in > 0:
        st.success(f"Budget = ${round((impressions*cpm_in)/1000,2)}")
    elif budget > 0 and cpm_in > 0:
        st.success(f"Impressions = {round((budget*1000)/cpm_in):,}")
    st.divider(); st.subheader("🗂️ Mapping Manager")
    mapping_manager_ui("GAM", all_options); st.markdown("")
    mapping_manager_ui("DCM", all_options)

# ============================================================
# FULL-VIEW MAPPING PANEL
# ============================================================
if st.session_state.show_fullview:
    fv_plat  = st.session_state.fullview_platform
    fv_camp  = st.session_state.fullview_campaign
    fv_pk    = platform_key(fv_plat, fv_camp)
    mappings = st.session_state.mappings
    hcol, ccol = st.columns([7,1])
    with hcol: st.subheader(f"⛶ Full Mapping View — {fv_plat} / {fv_camp}")
    with ccol:
        if st.button("✖ Close", use_container_width=True):
            st.session_state.show_fullview = False; st.rerun()
    if fv_pk in mappings:
        fv_df  = pd.DataFrame(list(mappings[fv_pk].items()), columns=["Keyword","Mapped Value"])
        search = st.text_input("🔎 Filter…", key="fv_search_main")
        if search:
            fv_df = fv_df[fv_df["Keyword"].str.contains(search, case=False, na=False) |
                          fv_df["Mapped Value"].str.contains(search, case=False, na=False)]
        st.dataframe(fv_df, use_container_width=True, height=500)
        st.caption(f"{len(fv_df)} of {len(mappings[fv_pk])} keyword(s) shown")
        st.download_button("⬇️ Download as CSV", data=fv_df.to_csv(index=False).encode(),
                           file_name=f"{fv_camp}_{fv_plat}_mapping.csv", mime="text/csv")
    else:
        st.warning("No mapping data found.")
    st.divider()

# ============================================================
# MAIN TABS
# ============================================================
tab_main, tab_explorer, tab_insights, tab_forecast, tab_reconcile = st.tabs([
    "📊 Reporting","🔍 Column Explorer","💡 Insights","🔮 Forecast","⚖️ GAM vs DCM"
])

# ─────────────────────────────────────────────
# TAB 1 — REPORTING
# ─────────────────────────────────────────────
with tab_main:
    st.markdown(f'<div class="section-title">Upload Files — {option}</div>', unsafe_allow_html=True)
    uc1, uc2 = st.columns(2)
    with uc1: gam_file = st.file_uploader("📂 GAM Report", type=["csv","xlsx"], key="gam_upload")
    with uc2: dcm_file = st.file_uploader("📂 DCM Report", type=["csv","xlsx"], key="dcm_upload")

    gam_df = read_uploaded_file(gam_file, "gam")
    dcm_df = read_uploaded_file(dcm_file, "dcm")

    if gam_df is not None:
        with st.expander("GAM Raw Preview"):
            st.dataframe(gam_df.head(20), use_container_width=True)
    if dcm_df is not None:
        with st.expander("DCM Raw Preview"):
            st.dataframe(dcm_df.head(20), use_container_width=True)

    gam_result, gam_clean = None, None
    dcm_result, dcm_clean = None, None
    if gam_df is not None:
        gam_result, gam_clean = process_data(gam_df, "GAM", option, date_range)
    if dcm_df is not None:
        dcm_result, dcm_clean = process_data(dcm_df, "DCM", option, date_range)

    st.markdown('<div class="section-title">Select Metrics</div>', unsafe_allow_html=True)
    pc1, pc2 = st.columns(2)
    with pc1: gam_pivot = build_pivot(gam_clean, "GAM") if gam_clean is not None else None
    with pc2: dcm_pivot = build_pivot(dcm_clean, "DCM") if dcm_clean is not None else None

    st.markdown('<div class="section-title">Results</div>', unsafe_allow_html=True)

    def display_result(pivot, platform):
        if pivot is None: st.info(f"No {platform} data processed yet."); return
        st.markdown(f'<span class="chip">{platform}</span>', unsafe_allow_html=True)
        st.dataframe(pivot, use_container_width=True)
        gc       = pivot.columns[0]
        chart_df = pivot[~pivot[gc].astype(str).str.contains("total", case=False, na=False)]
        ic, cc, ctrc = f"{platform}_Impressions", f"{platform}_Clicks", f"{platform}_CTR (%)"
        r1, r2, r3 = st.columns(3)
        if ic in chart_df.columns:
            with r1: st.caption("Impressions"); st.bar_chart(chart_df.set_index(gc)[ic])
        if cc in chart_df.columns:
            with r2: st.caption("Clicks");      st.bar_chart(chart_df.set_index(gc)[cc])
        if ctrc in chart_df.columns:
            with r3: st.caption("CTR (%)");     st.bar_chart(chart_df.set_index(gc)[ctrc])

    rc1, rc2 = st.columns(2)
    with rc1: display_result(gam_pivot, "GAM")
    with rc2: display_result(dcm_pivot, "DCM")

    def trend_chart(n_df, platform):
        if n_df is None: return
        dc = next((c for c in n_df.columns if "date" in c.lower()), None)
        ic = next((c for c in n_df.columns if "impression" in c.lower()), None)
        if not dc or not ic: return
        n_df = n_df.copy()
        n_df[dc] = pd.to_datetime(n_df[dc], errors="coerce")
        trend = n_df.groupby(dc)[ic].sum().reset_index().sort_values(dc)
        trend = trend.rename(columns={ic: f"{platform} Impressions"})
        if len(trend) > 1:
            st.caption(f"{platform} Impression Trend"); st.line_chart(trend.set_index(dc))

    st.markdown('<div class="section-title">Trend Over Time</div>', unsafe_allow_html=True)
    tc1, tc2 = st.columns(2)
    with tc1: trend_chart(gam_clean, "GAM")
    with tc2: trend_chart(dcm_clean, "DCM")

    st.markdown('<div class="section-title">Download</div>', unsafe_allow_html=True)
    file_name = st.text_input("File name (without extension)", value=f"{option}_report")
    dl1, dl2  = st.columns(2)
    for piv, label, col in [(gam_pivot,"GAM",dl1),(dcm_pivot,"DCM",dl2)]:
        with col:
            if piv is not None:
                buf = io.BytesIO(); piv.to_excel(buf, index=False); buf.seek(0)
                st.download_button(f"⬇️ Download {label} Excel", data=buf,
                                   file_name=f"{file_name}_{label}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)

# ─────────────────────────────────────────────
# TAB 2 — COLUMN EXPLORER
# ─────────────────────────────────────────────
with tab_explorer:
    st.markdown('<div class="section-title">Column Explorer</div>', unsafe_allow_html=True)

    def show_explorer(df, label, col_key):
        st.markdown(f'<span class="chip">{label}</span>', unsafe_allow_html=True)
        if df is None:
            st.info(f"Upload {label} file in the Reporting tab first."); return
        selected = st.selectbox("Select column", df.columns, key=col_key)
        if selected:
            vals = df[selected].dropna().astype(str).unique()
            st.metric("Unique values", len(vals))
            st.dataframe(pd.DataFrame(vals, columns=[selected]).head(500), use_container_width=True)

    e1, e2 = st.columns(2)
    with e1: show_explorer(gam_df, "GAM", "exp_gam")
    with e2: show_explorer(dcm_df, "DCM", "exp_dcm")

# ─────────────────────────────────────────────
# TAB 3 — INSIGHTS
# ─────────────────────────────────────────────
with tab_insights:
    st.markdown('<div class="section-title">Performance Insights</div>', unsafe_allow_html=True)

    def show_insights(pivot, platform, col):
        with col:
            st.markdown(f'<span class="chip">{platform}</span>', unsafe_allow_html=True)
            ins_list = generate_insights(pivot, platform)
            if ins_list:
                for ins in ins_list:
                    if any(ins.startswith(e) for e in ["🚀","🌟","💡","👍"]): st.success(ins)
                    elif any(ins.startswith(e) for e in ["⚠️","📉"]):          st.warning(ins)
                    else:                                                        st.info(ins)
            else:
                st.info(f"Upload and process {platform} data in the Reporting tab first.")

    i1, i2 = st.columns(2)
    show_insights(gam_pivot, "GAM", i1)
    show_insights(dcm_pivot, "DCM", i2)

# ─────────────────────────────────────────────
# TAB 4 — FORECAST  (two sub-tabs: XGBoost | Prophet)
# ─────────────────────────────────────────────
with tab_forecast:
    st.markdown('<div class="section-title">🔮 Impression & Click Forecast</div>',
                unsafe_allow_html=True)

    # Model availability banner
    col_xgb_stat, col_prp_stat = st.columns(2)
    with col_xgb_stat:
        if XGB_OK:
            st.success("✅ XGBoost — ready  (works with 7+ days of data, fast)")
        else:
            st.error(f"❌ XGBoost — {XGB_ERROR}")
    with col_prp_stat:
        if PROPHET_OK:
            st.success("✅ Prophet — ready  (needs 10+ days, slower but captures seasonality)")
        else:
            st.warning(f"⚠️ Prophet — {PROPHET_ERROR}")

    # Model comparison guide
    with st.expander("📖 Which model should I use? — Prophet vs XGBoost", expanded=False):
        st.markdown("""
| Feature | XGBoost ⚡ | Prophet 📈 |
|---|---|---|
| **Minimum data** | 7 days | 10 days |
| **Best at** | Short campaigns, quick patterns | Long campaigns with weekly seasonality |
| **Accuracy (small data)** | Better | Can overfit or be unstable |
| **Accuracy (28+ days)** | Good | Excellent — picks up weekly patterns |
| **Fake 100% accuracy?** | No — uses held-out validation | No — uses held-out validation |

**Recommendation:**
- Campaign running < 3 weeks → use **XGBoost**
- Campaign running 4+ weeks → try **both and compare**
- Both models use held-out validation for honest accuracy scores

### How Low / High are calculated
**XGBoost:** trains on history, measures day-to-day prediction errors (residuals), bootstraps them 500×, takes 10th–90th percentile → 80% confidence band.

**Prophet:** uses Bayesian uncertainty sampling (500 Monte Carlo simulations) to estimate a natural spread around the forecast trend → 80% confidence band.

Both methods give the same interpretation:
> *"We are 80% confident the real value on that day will fall between Low and High."*
""")

    # Sub-tabs for each model
    subtab_xgb, subtab_prophet = st.tabs(["⚡ XGBoost", "📈 Prophet"])

    with subtab_xgb:
        if not XGB_OK:
            st.error(f"⚠️ {XGB_ERROR}")
            st.markdown("""
**Streamlit Cloud:** add `xgboost` to `requirements.txt` → push → redeploy.

**Local (any OS):**
```bash
pip install xgboost
```
No compiler needed — just works.
""")
        else:
            render_forecast_ui("XGBoost", forecast_xgb, "xgb",
                               gam_clean, dcm_clean, option)

    with subtab_prophet:
        if not PROPHET_OK:
            st.error(f"⚠️ {PROPHET_ERROR}")
            st.markdown("""
**Streamlit Cloud:** add `prophet` to `requirements.txt` → push → redeploy.

**Local Mac:** `brew install gcc && pip install prophet`

**Local Windows:** `conda install -c conda-forge prophet`

**Local Linux:** `sudo apt-get install -y gcc g++ && pip install prophet`
""")
        else:
            render_forecast_ui("Prophet", forecast_prophet, "prophet",
                               gam_clean, dcm_clean, option)

# ─────────────────────────────────────────────
# TAB 5 — RECONCILIATION
# ─────────────────────────────────────────────
with tab_reconcile:
    st.markdown('<div class="section-title">GAM vs DCM Reconciliation</div>', unsafe_allow_html=True)

    final_df = None
    if gam_pivot is not None and dcm_pivot is not None:
        final_df = pd.merge(gam_pivot, dcm_pivot, on="Product", how="outer").fillna(0)
        g_imp, d_imp = "GAM_Impressions", "DCM_Impressions"
        if g_imp in final_df.columns and d_imp in final_df.columns:
            final_df["Discrepancy (%)"] = (
                (final_df[g_imp] - final_df[d_imp]) /
                final_df[g_imp].replace(0,1) * 100).round(2)
            final_df["Flag"] = final_df["Discrepancy (%)"].apply(
                lambda x: "⚠️ High" if abs(x) > 5 else "✅ OK")
            m1,m2,m3,m4 = st.columns(4)
            m1.metric("GAM Impressions", f"{int(final_df[g_imp].sum()):,}")
            m2.metric("DCM Impressions", f"{int(final_df[d_imp].sum()):,}")
            m3.metric("Avg Discrepancy", f"{round(final_df['Discrepancy (%)'].mean(),2)}%")
            m4.metric("⚠️ High Flags",   int((final_df["Flag"]=="⚠️ High").sum()))
        st.dataframe(final_df, use_container_width=True)
        if g_imp in final_df.columns and d_imp in final_df.columns:
            st.markdown('<div class="section-title">Impression Comparison</div>', unsafe_allow_html=True)
            chart_rec = final_df[~final_df["Product"].astype(str).str.contains("total",case=False,na=False)]
            st.bar_chart(chart_rec.set_index("Product")[[g_imp, d_imp]])
        buf = io.BytesIO(); final_df.to_excel(buf, index=False); buf.seek(0)
        st.download_button("⬇️ Download Reconciliation Report", data=buf,
                           file_name=f"{option}_reconciliation.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
    elif gam_pivot is not None:
        st.info("DCM not uploaded — showing GAM only.")
        st.dataframe(gam_pivot, use_container_width=True)
    elif dcm_pivot is not None:
        st.info("GAM not uploaded — showing DCM only.")
        st.dataframe(dcm_pivot, use_container_width=True)
    else:
        st.info("Upload both GAM and DCM files in the **Reporting** tab to see reconciliation here.")
