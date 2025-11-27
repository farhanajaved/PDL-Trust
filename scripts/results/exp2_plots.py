#!/usr/bin/env python3
"""
exp2_plots.py — Final Experiment-2 visualization (two CDF panels + boxplot)

Generates:
  • trev_cdf_panels.png   → two smooth-CDF panels (Δ_snap = 15 s and 60 s)
  • trev_box.png           → simple boxplot grouped by Δ_snap
  • exp2_trev_summary.tex  → summary statistics table

System takeaway:
  T_rev ≈ TTC_reg_s + snap_wait_s, dominated by snapshot cadence (Δ_snap).

How to run: 
python3 /home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results/exp2_plots.py \
    --csv /home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results/exp2_trev_20251107-124406.csv \
    --outdir ./exp2_out \
    --cadences 15,60 \
    --dpi 240

"""

import argparse, os, sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------------------------------------
# CLI + constants
# -------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Two-panel smooth CDF + boxplot for Experiment-2")
    p.add_argument("--csv", required=True, help="Path to Experiment-2 CSV")
    p.add_argument("--outdir", default="./exp2_out", help="Output directory")
    p.add_argument("--cadences", default="15,60", help="Δ_snap values (comma-separated)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dpi", type=int, default=240)
    return p.parse_args()

CANON = {
    "eventType": ["eventType","event_type"],
    "deltaSnap_s": ["deltaSnap_s","delta_snap_s"],
    "T_rev_s": ["T_rev_s","Trev_s","T_rev"],
    "TTC_reg_s": ["TTC_reg_s"],
    "snap_wait_s": ["snap_wait_s"],
    "ok": ["ok","success"]
}

# -------------------------------------------------------------
# Helpers
# -------------------------------------------------------------
def ensure_outdir(p): os.makedirs(p, exist_ok=True)

def map_columns(df):
    renamed={}
    for canon,syns in CANON.items():
        for s in syns:
            if s in df.columns:
                renamed[s]=canon; break
    return df.rename(columns=renamed)

def coerce_nonneg(s):
    s=pd.to_numeric(s,errors="coerce")
    return s.mask(s<0,np.nan)

def canonicalize(df):
    for c in ["T_rev_s","TTC_reg_s","snap_wait_s","deltaSnap_s"]:
        if c in df.columns:
            df[c]=coerce_nonneg(df[c])
    if "ok" in df.columns:
        df["ok"]=pd.to_numeric(df["ok"],errors="coerce").fillna(0).astype(int)
    return df

def to_list(x): return [float(v.strip()) for v in x.split(",") if v.strip()]

def percentile(s,q):
    s=s.dropna()
    return float(np.percentile(s,q)) if not s.empty else np.nan

# -------------------------------------------------------------
# Smooth CDF utilities
# -------------------------------------------------------------
def _kde_pdf(x, samples, bw):
    diffs = (x[:,None]-samples[None,:])/bw
    pdf = np.exp(-0.5*diffs*diffs).sum(axis=1)/(samples.size*bw*np.sqrt(2*np.pi))
    return pdf

def _kde_cdf(samples, grid_pts=600):
    s=np.asarray(samples,dtype=float)
    s=s[~np.isnan(s)]
    if s.size==0: return None,None
    s_min,s_max=np.min(s),np.max(s)
    span=max(1.0,s_max-s_min)
    x=np.linspace(s_min-0.05*span,s_max+0.05*span,grid_pts)
    iqr=np.subtract(*np.percentile(s,[75,25]))
    sigma=np.std(s)
    a=min(sigma,iqr/1.34) if (sigma>0 and iqr>0) else max(sigma,iqr/1.34,1.0)
    bw=0.9*a*s.size**(-1/5)
    bw=max(bw,0.5)
    pdf=_kde_pdf(x,s,bw)
    dx=x[1]-x[0]
    cdf=np.cumsum(pdf)*dx
    cdf=np.clip(cdf/(cdf[-1] if cdf[-1]>0 else 1.0),0,1)
    return x,cdf

# -------------------------------------------------------------
# Two-panel smooth CDF plot
# -------------------------------------------------------------
def plot_cdf_panels(df, cadences, outpath, dpi):
    fig, axs = plt.subplots(1, len(cadences), figsize=(9,3.8), sharey=True)
    if len(cadences)==1: axs=[axs]

    for ax, delta in zip(axs, cadences):
        vals = df.loc[df["deltaSnap_s"] == delta, "T_rev_s"].dropna().values
        if vals.size == 0: continue
        x, F = _kde_cdf(vals)
        if x is None: continue
        ax.plot(x, F, color="royalblue", lw=2)
        mean, med, p95 = np.mean(vals), np.median(vals), percentile(pd.Series(vals),95)
        ax.plot(x, F, color="royalblue", lw=2, label="$T_{rev}$")
        ax.axvline(mean, color="r", ls="--", lw=1.2, label="Mean")
        ax.axvline(med,  color="g", ls="-.", lw=1.2, label="Median")
        ax.axvline(p95,  color="b", ls=":",  lw=1.2, label="95th Percentile")
        ax.set_xlabel(r"$T_{\mathrm{rev}}$ (s)")
        ax.set_xlim(min(x)-1, max(x)+1)
        ax.set_ylim(0,1)

            # Label box
        ax.text(
        0.95, 0.08, fr"$\Delta_{{snap}} = {int(delta)}\,\mathrm{{s}}$",
        transform=ax.transAxes,
        fontsize=9,
        ha="right", va="bottom",
        bbox=dict(facecolor="white", edgecolor="gray", boxstyle="round,pad=0.3")
        )


    axs[0].set_ylabel("CDF")
    axs[0].legend(frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(outpath, dpi=dpi)
    plt.close(fig)
    print(f"[CDF panels] Saved {outpath}")

# -------------------------------------------------------------
# Boxplot
# -------------------------------------------------------------
def _nice_ylim(arrays, pad_frac=0.08, pad_abs=1.0):
    v=np.concatenate([a for a in arrays if len(a)>0])
    lo,hi=np.min(v),np.max(v)
    pad=max(pad_abs,pad_frac*(hi-lo if hi>lo else 10))
    return max(0,lo-pad),hi+pad

def plot_box_by_cadence(df, cadences, outpath, dpi):
    data, labels=[],[]
    for c in cadences:
        vals=df.loc[df["deltaSnap_s"]==c,"T_rev_s"].dropna().values
        data.append(vals)
        labels.append(f"Δ_snap = {int(c)} s")

    fig, ax = plt.subplots(figsize=(6,5))
    ax.boxplot(
        data, labels=labels, patch_artist=True, showfliers=True,
        boxprops=dict(facecolor="royalblue", color="black", alpha=0.85),
        medianprops=dict(color="black", linewidth=1),
        whiskerprops=dict(color="black"),
        capprops=dict(color="black")
    )
    y0,y1=_nice_ylim(data)
    ax.set_ylim(y0,y1)
    ax.set_ylabel(r"$T_{\mathrm{rev}}$ (s)")
    ax.set_xlabel(r"$\Delta_{\mathrm{snap}}$ [s]")
    fig.tight_layout()
    fig.savefig(outpath, dpi=dpi)
    plt.close(fig)
    print(f"[Box] Saved {outpath}")

# -------------------------------------------------------------
# Stats + TeX table
# -------------------------------------------------------------
def compute_stats(df):
    rows=[]
    for (cad,),g in df.groupby(["deltaSnap_s"]):
        trev=g["T_rev_s"].dropna()
        if trev.empty: continue
        rec={"deltaSnap_s":cad,"n":len(trev),
             "median":np.median(trev),
             "p95":percentile(trev,95),
             "p99":percentile(trev,99),
             "med_ttc":percentile(g["TTC_reg_s"],50) if "TTC_reg_s" in g else np.nan,
             "med_snap":percentile(g["snap_wait_s"],50) if "snap_wait_s" in g else np.nan}
        rows.append(rec)
    return pd.DataFrame(rows)

def write_tex_table(stats,outdir):
    if stats.empty: 
        print("[TeX] No stats, skipping."); return
    lines=[
r"\begin{table}[t]",
r"\centering",
r"\caption{Registry propagation summary per cadence (aggregated).}",
r"\label{tab:exp2_trev_summary_simple}",
r"\begin{tabular}{@{}r S[table-format=4.0] S[table-format=2.2] S[table-format=2.2] l@{}}",
r"\toprule",
r"{$\Delta_{\text{snap}}$ [s]} & {n} & {Median [s]} & {P95 [s]} & {Med. TTC / Med. Snap [s]} \\",
r"\midrule"]
    for _,r in stats.iterrows():
        ds=int(r["deltaSnap_s"]); n=int(r["n"])
        lines.append(f"{ds} & {n} & {r['median']:.2f} & {r['p95']:.2f} & {r['med_ttc']:.2f}~/{r['med_snap']:.2f} \\\\")
    lines.append(r"\bottomrule\n\end{tabular}\n\end{table}")
    with open(os.path.join(outdir,"exp2_trev_summary.tex"),"w") as f:
        f.write("\n".join(lines))
    print(f"[TeX] Wrote exp2_trev_summary.tex")

# -------------------------------------------------------------
# Main
# -------------------------------------------------------------
def main():
    args=parse_args()
    np.random.seed(args.seed)
    ensure_outdir(args.outdir)
    if not os.path.isfile(args.csv):
        sys.exit(f"CSV not found: {args.csv}")

    df=pd.read_csv(args.csv)
    df=map_columns(df)
    df=canonicalize(df)
    if "T_rev_s" not in df.columns:
        sys.exit("No T_rev_s column found.")
    if "ok" in df.columns:
        df=df[df["ok"]==1]

    cadences=to_list(args.cadences)
    df=df[df["deltaSnap_s"].isin(cadences)]
    if df.empty:
        sys.exit("No rows after filtering.")
    print(f"Loaded {len(df)} rows after filtering.")

    plot_cdf_panels(df, cadences, os.path.join(args.outdir,"trev_cdf_panels.png"), args.dpi)
    plot_cdf_panels(df, cadences, os.path.join(args.outdir,"trev_cdf_panels.svg"), args.dpi)
    plot_cdf_panels(df, cadences, os.path.join(args.outdir,"trev_cdf_panels.pdf"), args.dpi)
    plot_box_by_cadence(df, cadences, os.path.join(args.outdir,"trev_box.png"), args.dpi)
    plot_box_by_cadence(df, cadences, os.path.join(args.outdir,"trev_box.svg"), args.dpi)
    plot_box_by_cadence(df, cadences, os.path.join(args.outdir,"trev_box.pdf"), args.dpi)

    stats=compute_stats(df)
    write_tex_table(stats,args.outdir)
    print("Done.")

if __name__=="__main__":
    main()
