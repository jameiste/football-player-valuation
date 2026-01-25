### File to generate over all scores for each player ###
"""
Compare players across their:
    - position
    - league
    - age group
Make an overall score how well they perform and give a final score (like Fifa)
Besides that we want maybe sub-scores in various categories but that is secondary

Furthermore based on soft and hard factors a transfer market value is approximated and compared to its real value
"""
# Imports
import pandas as pd
import numpy as np

# Local imports
from functions.utils import load_excel, store_excel, update_sheets
from functions.data_related import standardize_data, context_features, zscore_within_group, numeric_values_string
from backend.metric_analyzation.prediction import compute_group_embeddings, train_lgbm, predict_lgbm, train_hgb, train_ridge, predict_hgb, predict_ridge, ensemble_mean
from environment.variable import STATS_NAME, MARKET_SHEET_NAME, POSITION_NAME, FEATURES_SCHEMA_SOFASCORE

# Function: Build up the scoring
def prepare_scoring():
    # Load data
    tm = load_excel(STATS_NAME, MARKET_SHEET_NAME)
    df = load_excel(STATS_NAME, "All")
    # Grouping
    groups = {
        "League": df.League.unique().tolist(),
        "Age": [np.arange(1,19,.1), np.arange(19,23,.1),
                np.arange(23,30,.1), np.arange(30,42,.1)],
        "Pos_group": df.Pos_group.unique().tolist(),
    }

    out = []

    for pos, schema in FEATURES_SCHEMA_SOFASCORE.items():
        # Position slice
        d = df[df.Pos_group == pos]
        feats = [f for g in schema.values() for f in g]

        # Context-standardized features
        X = pd.concat(
            [standardize_data(d, feats, grp, col) for col, grp in groups.items()],
            axis=1
        ).loc[:, ~pd.concat(
            [standardize_data(d, feats, grp, col) for col, grp in groups.items()],
            axis=1
        ).columns.duplicated()]

        # New column writing
        std_feats = []
        for base in feats:
            for prefix in ("League.", "Age.", "Pos_group."):
                col = f"{prefix}{base}"
                if col in X.columns:
                    std_feats.append(col)
        std_feats = list(dict.fromkeys(std_feats))

        # Align index
        X_aligned = X.reset_index(drop=True)
        d_aligned = d.reset_index(drop=True)

        # Add further features
        ctx_base = context_features(d_aligned)
        ctx_cols = [c for c in ctx_base.columns if c.startswith("ctx.")]
        X_ctx = zscore_within_group(
            ctx_base,
            cols=ctx_cols,
            group_cols=["League"],     # or ["League","Pos_group"]
            prefix="LeagueCtx"
        ).reset_index(drop=True)

        # Embeddings
        embeddings = compute_group_embeddings(d_aligned, schema, pos)

        # keep only embedding cols and align index too
        E_emb = embeddings.filter(like="_emb_").reset_index(drop=True)

        # Combine
        X_emb = pd.concat([X_aligned, E_emb], axis=1)

        # Target
        X_emb["Market_Value_EUR"] = d_aligned["Market_Value_EUR"].values
        y = np.log1p(X_emb["Market_Value_EUR"])

        # Matrix
        X_std = X_aligned[std_feats].copy()
        X_e   = X_emb.filter(like="_emb_").copy()

        # Ensure numeric
        for c in X_std.columns:
            X_std[c] = pd.to_numeric(X_std[c], errors="coerce")
        for c in X_e.columns:
            X_e[c] = pd.to_numeric(X_e[c], errors="coerce")

        # Drop NaN columns
        X_std = X_std.dropna(axis=1, how="all")
        X_e   = X_e.dropna(axis=1, how="all")

        # Keep only important columns
        const_std = [c for c in X_std.columns if X_std[c].nunique(dropna=True) <= 1]
        const_e   = [c for c in X_e.columns   if X_e[c].nunique(dropna=True) <= 1]
        if const_std:
            X_std = X_std.drop(columns=const_std)
        if const_e:
            X_e = X_e.drop(columns=const_e)

        # Fill empty data
        X_std = X_std.fillna(X_std.median(numeric_only=True))
        X_e   = X_e.fillna(X_e.median(numeric_only=True))

        # embeddings can be empty; handle gracefully
        has_emb = X_e.shape[1] > 0

        # Models
        m_raw   = train_lgbm(X_std, y)
        m_hgb   = train_hgb(X_std, y)
        m_ridge = train_ridge(X_std, y)

        if has_emb:
            m_emb = train_lgbm(X_e, y)

        # Predictions
        p_raw   = predict_lgbm(m_raw, X_std)
        p_hgb   = predict_hgb(m_hgb, X_std)
        p_ridge = predict_ridge(m_ridge, X_std)

        if has_emb:
            p_emb = predict_lgbm(m_emb, X_e)
        else:
            p_emb = np.zeros(len(y), dtype=float)

        # Ensemble
        X_emb["pred_log_value"] = ensemble_mean(p_raw, p_emb, p_hgb, p_ridge, weights=[0.35, 0.35, 0.20, 0.10])
        X_emb["pred_market_value"] = np.expm1(X_emb["pred_log_value"]).astype(int)
        X_emb["pred_market_value_€"] = numeric_values_string(value = X_emb["pred_market_value"])
        out.append(X_emb)
        store_excel(X_emb, POSITION_NAME, pos)

    # Final aggregation
    out = pd.concat(out).reset_index(drop=True)
    out["value_percentile"] = out.groupby("Pos_group")["pred_market_value"].rank(pct=True)

    store_excel(out, POSITION_NAME, "All")


# Function: Run the model prediction
def run_scoring():
    if update_sheets(offset_date=-1):
        prepare_scoring()
    return None