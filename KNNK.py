import streamlit as st
import pandas as pd
import io
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="KN & NK EDA Reporting App",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# MINIMAL CSS — zero hardcoded colors, pure Streamlit theme
# Only structural/layout tweaks here. All text/bg colors are
# inherited from Streamlit's active theme automatically.
# ============================================================
st.markdown("""
<style>

/* Section title */
.section-title {
    font-size: 1.05rem;
    font-weight: 700;
    border-bottom: 1px solid rgba(128,128,128,0.3);
    padding-bottom: 0.3rem;
    margin: 1.1rem 0 0.7rem;
}

/* Insight cards */
.insight-card {
    border-left: 3px solid rgba(128,128,128,0.4);
    border-radius: 0 6px 6px 0;
    padding: 0.55rem 0.9rem;
    margin: 0.3rem 0;
    font-size: 0.91rem;
}

/* Chips */
.chip {
    display: inline-block;
    border: 1px solid rgba(128,128,128,0.4);
    border-radius: 20px;
    padding: 0.15rem 0.65rem;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    margin-bottom: 0.4rem;
}

/* Dataframe width */
div[data-testid="stDataFrame"] {
    width: 100% !important;
}

</style>
""", unsafe_allow_html=True)

# ============================================================
# HEADER — uses native st.title so it respects theme
# ============================================================
st.title("📊 KN & NK EDA Reporting App")
st.caption("GAM + DCM Reconciliation · Campaign Mapping · Insights · Trend Analysis")
st.divider()

# ============================================================
# GOOGLE SHEETS CONNECTION
# ============================================================
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

@st.cache_resource
def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPE
    )
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
# Mappings are stored with a platform prefix key:
#   "GAM::Direct-NA-26-1641"  →  {keyword: value, ...}
#   "DCM::Direct-NA-26-1641"  →  {keyword: value, ...}
# This keeps them in the same sheet but clearly separated.
# ============================================================
def load_mappings():
    if not SHEET_OK:
        return {}
    try:
        rows, result = sheet.get_all_records(), {}
        for row in rows:
            campaign = str(row.get("Campaign", "")).strip()
            keyword  = str(row.get("Keyword",  "")).strip()
            value    = str(row.get("Value",    "")).strip()
            if not campaign or not keyword:
                continue
            result.setdefault(campaign, {})[keyword] = value
        return result
    except Exception as e:
        st.error(f"Error loading mappings: {e}")
        return {}

def save_mappings(mappings):
    if not SHEET_OK:
        return
    try:
        sheet.clear()
        sheet.append_row(["Campaign", "Keyword", "Value"])
        for campaign, kv in mappings.items():
            for key, value in kv.items():
                sheet.append_row([campaign, key, value])
    except Exception as e:
        st.error(f"Error saving mappings: {e}")

def platform_key(platform, campaign):
    """Composite key: 'GAM::Direct-NA-26-1641'"""
    return f"{platform}::{campaign}"

def get_platform_mappings(mappings, platform, campaign):
    """Return keyword→value dict for a given platform+campaign."""
    return mappings.get(platform_key(platform, campaign), {})

# ============================================================
# SESSION STATE INIT
# ============================================================
if "mappings" not in st.session_state:
    st.session_state.mappings = load_mappings()
if "show_fullview" not in st.session_state:
    st.session_state.show_fullview        = False
    st.session_state.fullview_platform    = ""
    st.session_state.fullview_campaign    = ""

# ============================================================
# LEGACY HARDCODED FALLBACK MAPPINGS (GAM only by default)
# ============================================================
LEGACY_MAPPINGS = {
    platform_key("GAM", "Direct-NA-26-1641"): {
        "Thematic_Standard":     "EDit",
        "AV_Contextual_Banners": "AV",
        "5224631": "AI In Action On-Site Editorial Series Sponsorship (10x Editorial Articles) - Q2",
        "5224633": "AV Contextual Banners - Q2"
    },
    platform_key("GAM", "Direct-NA-25-1619"): {
        "Audience  FullScape":                       "Audience Targeted Custom Video Distribution FullScape Units",
        "Audience Marquee":                          "Audience Targeted Custom Video Distribution Marquee and Interlude Units",
        "Audience Interlude":                        "Audience Targeted Custom Video Distribution Marquee and Interlude Units",
        "Added Value Contextually Targeted Banners": "AV"
    },
    platform_key("GAM", "Direct-NA-25-1608"): {
        "Audience Standard":                       "CPM",
        "Contextual Targeted Banners":             "AV",
        "Tech Standard":                           "Tech Section",
        "Full Site Business Insider POE Standard": "Tech Section"
    },
    platform_key("GAM", "Direct-NA-26-1632"): {
        "Contextual Standard":  "AV",
        "Contextual Marquee":   "Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Thematic Interlude":   "Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Contextual Interlude": "Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Thematic Marquee":     "Contextual Thematic Targeted Custom Units Marquee and Interlude",
        "Contextual YouTube":   "Contextual Targeted Business Insider Video YouTube In-Stream",
        "Contextual Onsite":    "Contextual Targeted Business Insider Video Business Insider On-Site Pre-Roll",
        "Audience YouTube":     "Audience Targeted Business Insider Video YouTube In-Stream",
        "1P Audience Onsite":   "Audience Targeted Business Insider Video Business Insider On-Site Pre-Roll",
        "3P Audience Onsite":   "Audience Targeted Business Insider Video Business Insider On-Site Pre-Roll"
    },
    platform_key("GAM", "Direct-NA-25-1625"): {
        "Contextual_YouTube_National": "Contextual Targeted Business Insider YouTube In-Stream- Geo- National",
        "Contextual_On-Site_National": "Contextual Targeted Business Insider On-Site Pre-Roll- Geo- National",
        "AV_Contextual_Banners":       "AV",
        "Audience":                    "Audience Targeted Custom Units Marquee and Interlude Geo- National"
    },
    platform_key("GAM", "Direct-NA-25-1566"): {
        "CB2 Creative_Multi":    "Audience Targeted Custom Marquee and Interlude Units - CB2 Creative",
        "C&B Creative_Multi":    "Audience Targeted Custom Marquee and Interlude Units | C&B Creative",
        "CB2 Creative_AV_Multi": "Added Value Contextually Targeted Banners CB2 Creative",
        "C&B Creative_AV_Multi": "Added Value Contextually Targeted Banners C&B Creative"
    }
}

def get_active_mappings(platform, campaign):
    """GSheet first, legacy fallback second."""
    pk  = platform_key(platform, campaign)
    m   = st.session_state.mappings
    if pk in m and m[pk]:
        return m[pk]
    return LEGACY_MAPPINGS.get(pk, {})

# ============================================================
# OPTIONS
# ============================================================
DEFAULT_OPTIONS = [
    "Direct-NA-26-1641", "Direct-NA-25-1619", "Direct-NA-25-1608",
    "Direct-NA-26-1632", "Direct-NA-25-1625", "Direct-NA-25-1566"
]
# Derive campaigns from stored keys (strip platform prefix)
dynamic_campaigns = []
for k in st.session_state.mappings.keys():
    if "::" in k:
        dynamic_campaigns.append(k.split("::", 1)[1])
all_options = sorted(set(DEFAULT_OPTIONS + dynamic_campaigns))

# ============================================================
# CORE PROCESSING
# ============================================================
def process_data(df, platform, campaign, date_range=None):
    n_df = df.copy()

    if date_range:
        dc = next((c for c in n_df.columns if "date" in c.lower()), None)
        if dc:
            n_df[dc] = pd.to_datetime(n_df[dc], errors="coerce")
            s, e = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            n_df = n_df[(n_df[dc] >= s) & (n_df[dc] <= e)]

    n_df["Product"] = "Ignore"

    # Detect line-item column
    if "Line item" in n_df.columns:
        col_name = "Line item"
    elif "Package/Roadblock" in n_df.columns:
        col_name = "Package/Roadblock"
    elif "Placement" in n_df.columns:
        col_name = "Placement"
    else:
        st.warning(f"⚠️ {platform}: Required column ('Line item' / 'Package/Roadblock'/'Placement') not found.")
        return None, None

    # Detect metrics
    if platform == "GAM":
        imp_col   = "Ad server impressions" if "Ad server impressions" in n_df.columns else "Impressions"
        click_col = "Ad server clicks"      if "Ad server clicks"      in n_df.columns else "Clicks"
    else:  # DCM
        imp_col, click_col = "Impressions", "Clicks"

    if imp_col not in n_df.columns or click_col not in n_df.columns:
        st.warning(f"⚠️ {platform}: Columns '{imp_col}'/'{click_col}' not found.")
        return None, None

    # Clean + remove test rows
    n_df[col_name] = n_df[col_name].fillna("").astype(str)
    before  = len(n_df)
    n_df    = n_df[~n_df[col_name].str.lower().str.contains("test", na=False)]
    removed = before - len(n_df)
    if removed:
        st.info(f"🧹 {platform}: Removed {removed} 'test' row(s)")

    # Apply mappings
    active = get_active_mappings(platform, campaign)
    n_df["_cl"] = n_df[col_name].str.lower()
    for key, value in active.items():
        kc = str(key).strip().lower()
        if kc:
            n_df.loc[n_df["_cl"].apply(lambda x: kc in x), "Product"] = value
    n_df.drop(columns=["_cl"], inplace=True)

    result = n_df.groupby("Product")[[imp_col, click_col]].sum().reset_index()
    result = result.rename(columns={
        imp_col:   f"{platform}_Impressions",
        click_col: f"{platform}_Clicks"
    })
    return result, n_df

# ============================================================
# PIVOT BUILDER
# ============================================================
def build_pivot(n_df, platform, key_suffix=""):
    if n_df is None or "Product" not in n_df.columns:
        return None
    numeric_cols = n_df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    if not numeric_cols:
        return None

    defaults = [c for c in numeric_cols if any(
        kw in c.lower() for kw in ["impression", "click", "view", "reach"]
    )][:4]

    selected = st.multiselect(
        f"Metrics — {platform}",
        numeric_cols,
        default=defaults or numeric_cols[:2],
        key=f"metrics_{platform}_{key_suffix}"
    )
    if not selected:
        return None

    pivot = n_df.groupby("Product")[selected].sum().reset_index()
    pivot = pivot.rename(columns={
        "Ad server impressions": f"{platform}_Impressions",
        "Ad server clicks":      f"{platform}_Clicks",
        "Impressions":           f"{platform}_Impressions",
        "Clicks":                f"{platform}_Clicks"
    })

    ic = f"{platform}_Impressions"
    cc = f"{platform}_Clicks"
    if ic in pivot.columns and cc in pivot.columns:
        pivot[f"{platform}_CTR (%)"] = (
            pivot[cc] / pivot[ic].replace(0, np.nan) * 100
        ).fillna(0).round(2)

    total            = pivot.select_dtypes(include="number").sum()
    total["Product"] = "TOTAL"
    pivot = pd.concat([pivot, pd.DataFrame([total])], ignore_index=True)
    return pivot

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
    px        = f"[{platform}] " if platform else ""

    insights.append(f"📊 {px}Total Impressions: {int(total_imp):,} | Total Clicks: {int(total_clk):,}")

    # ================= CTR INSIGHTS =================
    if ctr_c and not data.empty:

        emoji = "🚀" if avg_ctr > 2 else ("👍" if avg_ctr > 1 else "⚠️")
        label = "Strong" if avg_ctr > 2 else ("Moderate" if avg_ctr > 1 else "Low")
        insights.append(f"{emoji} {px}{label} avg CTR: {round(avg_ctr, 2)}%")

        sorted_ctr = data.sort_values(ctr_c, ascending=False)

        # ✅ SAFE: Top performer
        if len(sorted_ctr) > 0:
            best = sorted_ctr.iloc[0]
            insights.append(f"🔥 {px}Top performer: {best[gc]} ({round(best[ctr_c], 2)}%)")

        # ✅ SAFE: Lowest performer
        if len(sorted_ctr) > 1:
            worst = sorted_ctr.iloc[-1]
            insights.append(f"📉 {px}Lowest: {worst[gc]} ({round(worst[ctr_c], 2)}%)")

        # ✅ SAFE: Under/High performers
        low_names = ", ".join(
            data[data[ctr_c] < avg_ctr][gc].astype(str).head(3)
        )
        high_names = ", ".join(
            data[data[ctr_c] >= avg_ctr][gc].astype(str).head(3)
        )

        if low_names:
            insights.append(f"📉 {px}Underperformers: {low_names}")
        if high_names:
            insights.append(f"🌟 {px}High performers: {high_names}")


    # ================= IMPRESSION INSIGHTS =================
    if imp_c and total_imp > 0 and not data.empty:

        sorted_imp = data.sort_values(imp_c, ascending=False)

        # ✅ SAFE: Highest impressions
        if len(sorted_imp) > 0:
            top_row = sorted_imp.iloc[0]
            insights.append(f"📈 {px}Highest impressions: {top_row[gc]}")

        # ✅ SAFE: Share logic
        share = data[imp_c] / total_imp
        if not share.empty and share.max() > 0.5:
            insights.append(
                f"⚠️ {px}Heavy dependency on '{data.loc[share.idxmax(), gc]}' (>50%)"
            )


    # ================= RECOMMENDATION =================
    rec = (
        "💡 Improve creatives & targeting" if avg_ctr < 1
        else "💡 Optimize low performers, scale winners" if avg_ctr < 2
        else "💡 Scale top performers aggressively"
    )

    insights.append(f"{px}{rec}")

    return insights

# ============================================================
# FILE READER HELPER
# ============================================================
def read_uploaded_file(uploaded, key_prefix):
    if uploaded is None:
        return None
    if uploaded.name.endswith(".csv"):
        return pd.read_csv(uploaded)
    xls     = pd.ExcelFile(uploaded)
    default = "Ad Manager Report" if "Ad Manager Report" in xls.sheet_names else xls.sheet_names[0]
    chosen  = st.selectbox(
        f"Select sheet — {uploaded.name}",
        xls.sheet_names,
        index=xls.sheet_names.index(default),
        key=f"{key_prefix}_sheet"
    )
    return pd.read_excel(uploaded, sheet_name=chosen)

# ============================================================
# MAPPING UI HELPER (reusable for GAM and DCM)
# ============================================================
def mapping_manager_ui(platform, all_campaign_options):
    """Renders Add/Update, View, Delete UI for one platform."""
    mappings = st.session_state.mappings

    st.markdown(f"##### {platform} Mapping")

    with st.expander(f"➕ Add / Update {platform} Mapping"):
        camp_id   = st.text_input("Campaign ID", key=f"{platform}_add_id")
        pk        = platform_key(platform, camp_id) if camp_id else ""
        if camp_id and pk in mappings:
            st.caption("Existing mappings:")
            st.json(mappings[pk])
        clear_old = st.checkbox("Replace existing", key=f"{platform}_clear")
        bulk_text = st.text_area(
            "keyword = value  (one per line)",
            height=140,
            placeholder="Audience = Audience Data\nAV = Added Value",
            key=f"{platform}_bulk"
        )
        if st.button(f"💾 Save {platform} Mapping", use_container_width=True, key=f"{platform}_save"):
            if not camp_id.strip():
                st.error("Enter Campaign ID")
            else:
                mappings[pk] = {} if clear_old else mappings.get(pk, {})
                added, skipped = 0, 0
                for line in bulk_text.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip()
                        if k and v:
                            mappings[pk][k] = v
                            added += 1
                        else:
                            skipped += 1
                    else:
                        skipped += 1
                save_mappings(mappings)
                st.session_state.mappings = mappings
                st.success(f"✅ {added} mapping(s) saved")
                if skipped:
                    st.warning(f"⚠️ {skipped} line(s) skipped")
                st.rerun()

    # ── View (compact + full-view button)
    with st.expander(f"👁️ View {platform} Mapping"):
        platform_keys = [k for k in mappings if k.startswith(f"{platform}::")]
        if platform_keys:
            display_labels = [k.split("::", 1)[1] for k in platform_keys]
            chosen_label   = st.selectbox("Campaign", display_labels, key=f"{platform}_view_sel")
            chosen_pk      = platform_key(platform, chosen_label)
            if chosen_pk in mappings:
                mdf = pd.DataFrame(
                    list(mappings[chosen_pk].items()),
                    columns=["Keyword", "Mapped Value"]
                )
                # Native Streamlit dataframe — user can expand via the ⛶ icon built into the widget
                st.dataframe(mdf, use_container_width=True, height=200)

                if st.button(
                    f"⛶ Full View — {chosen_label}",
                    use_container_width=True,
                    key=f"{platform}_fullview_btn"
                ):
                    st.session_state.show_fullview     = True
                    st.session_state.fullview_platform = platform
                    st.session_state.fullview_campaign = chosen_label
                    st.rerun()
        else:
            st.info(f"No {platform} mappings saved yet.")

    # ── Delete
    with st.expander(f"🗑️ Delete {platform} Campaign"):
        platform_keys = [k for k in mappings if k.startswith(f"{platform}::")]
        if platform_keys:
            del_labels = [k.split("::", 1)[1] for k in platform_keys]
            del_label  = st.selectbox("Campaign to delete", del_labels, key=f"{platform}_del_sel")
            if st.button(f"❌ Delete {del_label}", use_container_width=True, key=f"{platform}_del_btn"):
                del mappings[platform_key(platform, del_label)]
                save_mappings(mappings)
                st.session_state.mappings = mappings
                st.success("Deleted ✅")
                st.rerun()
        else:
            st.info(f"No {platform} campaigns to delete.")

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    if st.button("🔄 Refresh Mappings", use_container_width=True):
        st.session_state.mappings = load_mappings()
        st.toast("Mappings refreshed ✅")
        st.rerun()

    st.divider()
    st.header("⚙️ Select Campaign ID for Reporting")
    option = st.selectbox("Campaign / Report", all_options)

    st.divider()
    st.subheader("📅 Date Filter")
    apply_date = st.checkbox("Enable Date Filter")
    date_range = None
    if apply_date:
        c1, c2    = st.columns(2)
        with c1: sd = st.date_input("Start", key="sd")
        with c2: ed = st.date_input("End",   key="ed")
        date_range = (sd, ed)

    st.divider()
    st.subheader("🧮 CPM Calculator")
    budget      = st.number_input("Budget ($)",  min_value=0.0, step=100.0)
    impressions = st.number_input("Impressions", min_value=0.0, step=1000.0)
    cpm_in      = st.number_input("CPM ($)",     min_value=0.0, step=0.1)
    if budget > 0 and impressions > 0:
        st.success(f"CPM = ${round((budget / impressions) * 1000, 2)}")
    elif impressions > 0 and cpm_in > 0:
        st.success(f"Budget = ${round((impressions * cpm_in) / 1000, 2)}")
    elif budget > 0 and cpm_in > 0:
        st.success(f"Impressions = {round((budget * 1000) / cpm_in):,}")

    st.divider()
    st.subheader("🗂️ Mapping Manager")
    mapping_manager_ui("GAM", all_options)
    st.markdown("")  # spacing
    mapping_manager_ui("DCM", all_options)

# ============================================================
# FULL-VIEW MAPPING PANEL (above tabs, dismissible)
# ============================================================
if st.session_state.show_fullview:
    fv_plat = st.session_state.fullview_platform
    fv_camp = st.session_state.fullview_campaign
    fv_pk   = platform_key(fv_plat, fv_camp)
    mappings = st.session_state.mappings

    hcol, ccol = st.columns([7, 1])
    with hcol:
        st.subheader(f"⛶ Full Mapping View — {fv_plat} / {fv_camp}")
    with ccol:
        if st.button("✖ Close", use_container_width=True):
            st.session_state.show_fullview = False
            st.rerun()

    if fv_pk in mappings:
        fv_df = pd.DataFrame(
            list(mappings[fv_pk].items()),
            columns=["Keyword", "Mapped Value"]
        )
        search = st.text_input("🔎 Filter…", key="fv_search_main")
        if search:
            fv_df = fv_df[
                fv_df["Keyword"].str.contains(search, case=False, na=False) |
                fv_df["Mapped Value"].str.contains(search, case=False, na=False)
            ]

        # Native Streamlit dataframe — has built-in ⛶ fullscreen button top-right
        st.dataframe(fv_df, use_container_width=True, height=500)
        st.caption(f"{len(fv_df)} of {len(mappings[fv_pk])} keyword(s) shown")

        buf = fv_df.to_csv(index=False).encode()
        st.download_button(
            "⬇️ Download as CSV",
            data=buf,
            file_name=f"{fv_camp}_{fv_plat}_mapping.csv",
            mime="text/csv"
        )
    else:
        st.warning("No mapping data found.")
    st.divider()

# ============================================================
# MAIN TABS
# ============================================================
tab_main, tab_explorer, tab_insights, tab_reconcile = st.tabs([
    "📊 Reporting", "🔍 Column Explorer", "💡 Insights", "⚖️ GAM vs DCM"
])

# ──────────────────────────────────────────────────────────────
# TAB 1 — REPORTING
# ──────────────────────────────────────────────────────────────
with tab_main:
    st.markdown(
        f'<div class="section-title">Upload Files — {option}</div>',
        unsafe_allow_html=True
    )

    uc1, uc2 = st.columns(2)
    with uc1:
        gam_file = st.file_uploader("📂 GAM Report", type=["csv", "xlsx"], key="gam_upload")
    with uc2:
        dcm_file = st.file_uploader("📂 DCM Report", type=["csv", "xlsx"], key="dcm_upload")

    gam_df = read_uploaded_file(gam_file, "gam")
    dcm_df = read_uploaded_file(dcm_file, "dcm")
# uploaded files are read into dataframes, which are then processed to extract insights and build pivot tables for reporting.
#  The UI allows users to explore columns, view trends, and download results, all while managing campaign mappings effectively.
    if gam_df is not None:
        with st.expander("GAM Raw Preview"):
            # Native Streamlit dataframe with built-in fullscreen button
            st.dataframe(gam_df.head(), use_container_width=True)
    if dcm_df is not None:
        with st.expander("DCM Raw Preview"):
            st.dataframe(dcm_df.head(), use_container_width=True)

    # Process both files
    gam_result, gam_clean = (None, None)
    dcm_result, dcm_clean = (None, None)
    if gam_df is not None:
        gam_result, gam_clean = process_data(gam_df, "GAM", option, date_range)
    if dcm_df is not None:
        dcm_result, dcm_clean = process_data(dcm_df, "DCM", option, date_range)

    # Metric selection
    st.markdown('<div class="section-title">Select Metrics</div>', unsafe_allow_html=True)
    pc1, pc2 = st.columns(2)
    with pc1:
        gam_pivot = build_pivot(gam_clean, "GAM") if gam_clean is not None else None
    with pc2:
        dcm_pivot = build_pivot(dcm_clean, "DCM") if dcm_clean is not None else None

    # Results
    st.markdown('<div class="section-title">Results</div>', unsafe_allow_html=True)

    def display_result(pivot, platform):
        if pivot is None:
            st.info(f"No {platform} data processed yet.")
            return
        st.markdown(f'<span class="chip">{platform}</span>', unsafe_allow_html=True)
        # Native dataframe — built-in ⛶ fullscreen via Streamlit's expand icon
        st.dataframe(pivot, use_container_width=True)

        gc       = pivot.columns[0]
        chart_df = pivot[~pivot[gc].astype(str).str.contains("total", case=False, na=False)]
        ic       = f"{platform}_Impressions"
        cc       = f"{platform}_Clicks"
        ctrc     = f"{platform}_CTR (%)"

        r1, r2, r3 = st.columns(3)
        if ic in chart_df.columns:
            with r1:
                st.caption("Impressions")
                st.bar_chart(chart_df.set_index(gc)[ic])
        if cc in chart_df.columns:
            with r2:
                st.caption("Clicks")
                st.bar_chart(chart_df.set_index(gc)[cc])
        if ctrc in chart_df.columns:
            with r3:
                st.caption("CTR (%)")
                st.bar_chart(chart_df.set_index(gc)[ctrc])

    rc1, rc2 = st.columns(2)
    with rc1:
        display_result(gam_pivot, "GAM")
    with rc2:
        display_result(dcm_pivot, "DCM")

    # Trend
    def trend_chart(n_df, platform):
        if n_df is None:
            return
        dc = next((c for c in n_df.columns if "date" in c.lower()), None)
        ic = next((c for c in n_df.columns if "impression" in c.lower()), None)
        if not dc or not ic:
            return
        n_df = n_df.copy()
        n_df[dc] = pd.to_datetime(n_df[dc], errors="coerce")
        trend    = n_df.groupby(dc)[ic].sum().reset_index().sort_values(dc)
        trend    = trend.rename(columns={ic: f"{platform} Impressions"})
        if len(trend) > 1:
            st.caption(f"{platform} Impression Trend")
            st.line_chart(trend.set_index(dc))

    st.markdown('<div class="section-title">Trend Over Time</div>', unsafe_allow_html=True)
    tc1, tc2 = st.columns(2)
    with tc1:
        trend_chart(gam_clean, "GAM")
    with tc2:
        trend_chart(dcm_clean, "DCM")

    # Download
    st.markdown('<div class="section-title">Download</div>', unsafe_allow_html=True)
    file_name = st.text_input("File name (without extension)", value=f"{option}_report")
    dl1, dl2  = st.columns(2)
    for piv, label, col in [(gam_pivot, "GAM", dl1), (dcm_pivot, "DCM", dl2)]:
        with col:
            if piv is not None:
                buf = io.BytesIO()
                piv.to_excel(buf, index=False)
                buf.seek(0)
                st.download_button(
                    f"⬇️ Download {label} Excel",
                    data=buf,
                    file_name=f"{file_name}_{label}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

# ──────────────────────────────────────────────────────────────
# TAB 2 — COLUMN EXPLORER
# ──────────────────────────────────────────────────────────────
with tab_explorer:
    st.markdown('<div class="section-title">Column Explorer</div>', unsafe_allow_html=True)

    def show_explorer(df, label, col_key):
        st.markdown(f'<span class="chip">{label}</span>', unsafe_allow_html=True)
        if df is None:
            st.info(f"Upload {label} file in the Reporting tab first.")
            return
        selected = st.selectbox("Select column", df.columns, key=col_key)
        if selected:
            vals = df[selected].dropna().astype(str).unique()
            st.metric("Unique values", len(vals))
            # Built-in ⛶ expand icon on every st.dataframe
            st.dataframe(
                pd.DataFrame(vals, columns=[selected]).head(500),
                use_container_width=True
            )

    e1, e2 = st.columns(2)
    with e1:
        show_explorer(gam_df, "GAM", "exp_gam")
    with e2:
        show_explorer(dcm_df, "DCM", "exp_dcm")

# ──────────────────────────────────────────────────────────────
# TAB 3 — INSIGHTS
# ──────────────────────────────────────────────────────────────
with tab_insights:
    st.markdown('<div class="section-title">Performance Insights</div>', unsafe_allow_html=True)

    def show_insights(pivot, platform, col):
        with col:
            st.markdown(f'<span class="chip">{platform}</span>', unsafe_allow_html=True)
            ins_list = generate_insights(pivot, platform)
            if ins_list:
                for ins in ins_list:
                    # st.info / st.success etc. fully respect theme — use them instead of raw HTML
                    if ins.startswith("🚀") or ins.startswith("🌟") or ins.startswith("💡"):
                        st.success(ins)
                    elif ins.startswith("⚠️") or ins.startswith("📉"):
                        st.warning(ins)
                    elif ins.startswith("🔥") or ins.startswith("📈"):
                        st.info(ins)
                    else:
                        st.info(ins)
            else:
                st.info(f"Upload and process {platform} data in the Reporting tab first.")

    i1, i2 = st.columns(2)
    show_insights(gam_pivot, "GAM", i1)
    show_insights(dcm_pivot, "DCM", i2)

# ──────────────────────────────────────────────────────────────
# TAB 4 — RECONCILIATION
# ──────────────────────────────────────────────────────────────
with tab_reconcile:
    st.markdown('<div class="section-title">GAM vs DCM Reconciliation</div>', unsafe_allow_html=True)

    final_df = None
    if gam_pivot is not None and dcm_pivot is not None:
        final_df = pd.merge(gam_pivot, dcm_pivot, on="Product", how="outer").fillna(0)
        g_imp, d_imp = "GAM_Impressions", "DCM_Impressions"

        if g_imp in final_df.columns and d_imp in final_df.columns:
            final_df["Discrepancy (%)"] = np.where(
                (final_df[g_imp] + final_df[d_imp]) == 0,
                0,
                ((final_df[g_imp] - final_df[d_imp]) / 
                ((final_df[g_imp] + final_df[d_imp]) / 2)) * 100
            )

            final_df["Discrepancy (%)"] = final_df["Discrepancy (%)"].round(2)
            final_df["Flag"] = final_df["Discrepancy (%)"].apply(
                lambda x: "⚠️ High" if abs(x) > 3 else "✅ OK"
            )
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("GAM Impressions", f"{int(final_df[g_imp].sum()):,}")
            m2.metric("DCM Impressions", f"{int(final_df[d_imp].sum()):,}")
            m3.metric("Avg Discrepancy", f"{round(final_df['Discrepancy (%)'].mean(), 2)}%")
            m4.metric("⚠️ High Flags",   int((final_df["Flag"] == "⚠️ High").sum()))

        # Native dataframe with built-in fullscreen
        st.dataframe(final_df, use_container_width=True)

        if g_imp in final_df.columns and d_imp in final_df.columns:
            st.markdown('<div class="section-title">Impression Comparison</div>', unsafe_allow_html=True)
            chart_rec = final_df[
                ~final_df["Product"].astype(str).str.contains("total", case=False, na=False)
            ]
            st.bar_chart(chart_rec.set_index("Product")[[g_imp, d_imp]])

        buf = io.BytesIO()
        final_df.to_excel(buf, index=False)
        buf.seek(0)
        st.download_button(
            "⬇️ Download Reconciliation Report",
            data=buf,
            file_name=f"{option}_reconciliation.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    elif gam_pivot is not None:
        st.info("DCM file not uploaded — showing GAM only.")
        st.dataframe(gam_pivot, use_container_width=True)
    elif dcm_pivot is not None:
        st.info("GAM file not uploaded — showing DCM only.")
        st.dataframe(dcm_pivot, use_container_width=True)
    else:
        st.info("Upload both GAM and DCM files in the **Reporting** tab to see reconciliation here.")
