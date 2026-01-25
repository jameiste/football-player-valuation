### Contains function for prediciton model ###

# Imports
import pandas as pd
import lightgbm as lgb
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge

# Function: Computes group embeddings
def compute_group_embeddings(df: pd.DataFrame, feature_schema, pos_group, n_components=3) -> pd.DataFrame:
    """
    Returns a dataframe with embedding columns appended
    """
    pos_df = df[df["Pos_group"] == pos_group].copy()

    imp = SimpleImputer(strategy="median")

    for group_name, features in feature_schema.items():
        cols = [c for c in features if c in pos_df.columns]
        if len(cols) < 2:
            continue

        Xg = pos_df[cols].to_numpy()
        Xg = imp.fit_transform(Xg) 

        pca = PCA(n_components=min(n_components, len(cols)), random_state=42)
        emb = pca.fit_transform(Xg)

        for i in range(emb.shape[1]):
            pos_df[f"{group_name}_emb_{i}"] = emb[:, i]

    return pos_df



# Function: Use XGBoost for fit data
def train_predict_value(df, target_col="market_value"):
    feature_cols = [
        c for c in df.columns
        if c.endswith("_emb_0") or "_emb_" in c
    ]

    X = df[feature_cols].values
    y = np.log1p(df[target_col].values)

    model = lgb.LGBMRegressor(
        n_estimators=2000,
        learning_rate=0.03,
        num_leaves=64,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
    )

    model.fit(X, y)

    df["pred_log_value"] = model.predict(X)
    df["pred_market_value"] = np.expm1(df["pred_log_value"])

    return df, model


# Function: Train and predict by lgbm
def train_lgbm(X, y):
    m = lgb.LGBMRegressor(
        objective="regression",
        n_estimators=800,
        learning_rate=0.05,
        num_leaves=31,
        max_depth=-1,
        min_data_in_leaf=5,
        min_gain_to_split=0.0,
        min_sum_hessian_in_leaf=1e-3,
        feature_fraction=0.9,
        bagging_fraction=0.9,
        bagging_freq=1,
        lambda_l2=1.0,
        random_state=42,
        verbose=-1
    )
    m.fit(X, y)
    return m


def predict_lgbm(model, X):
    return model.predict(X)


# Function: Gradient Boosting
def train_hgb(X, y):
    m = HistGradientBoostingRegressor(
        max_depth=8,
        learning_rate=0.05,
        max_iter=2000,
        random_state=42,
    )
    m.fit(X, y)
    return m

def predict_hgb(m, X):
    return m.predict(X)

# Function: Ridge regression
def train_ridge(X, y):
    m = Ridge(alpha=3.0, random_state=42)
    m.fit(X, y)
    return m

def predict_ridge(m, X):
    return m.predict(X)

# Function: Build ensemble over multiple models
def ensemble_mean(*preds, weights=None):
    P = np.vstack(preds)

    if weights is None:
        return P.mean(axis=0)

    w = np.array(weights).reshape(-1, 1)
    return (P * w).sum(axis=0)