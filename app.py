import streamlit as st
import pandas as pd
import pickle
import json
import os
import base64
from datetime import datetime
import altair as alt

# -------------------------
# 🎯 Page Configuration
# -------------------------
st.set_page_config(page_title="BIN Predictor Dashboard", layout="wide", page_icon="altitudepay.svg")

# -------------------------
# 🖼️ Web-style Brand Header with Logo
# -------------------------
def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

logo_base64 = get_base64_image("altitudepaylogo.png")

st.markdown(f"""
    <style>
    .custom-header {{
        display: flex;
        align-items: center;
        padding: 12px 20px;
        background-color: #ffffff;
        border-bottom: 1px solid #eee;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
        position: sticky;
        top: 0;
        z-index: 999;
    }}
    .custom-header img {{
        height: 40px;
    }}
    </style>
    <div class="custom-header">
        <img src="data:image/png;base64,{logo_base64}" alt="AltitudePay Logo">
    </div>
""", unsafe_allow_html=True)

# -------------------------
# 🧠 Load Model & Stats
# -------------------------
@st.cache_resource
def load_artifacts():
    with open("processor_success_model.pkl", "rb") as f_model, \
         open("processor_success_stats.pkl", "rb") as f_stats, \
         open("processor_name_mapping.json", "r") as f_map:

        model = pickle.load(f_model)
        stats = pickle.load(f_stats)
        processor_name_map = json.load(f_map)
        reverse_map = {v: k for k, v in processor_name_map.items()}
    return model, stats, processor_name_map, reverse_map

model, stats, processor_name_map, reverse_processor_map = load_artifacts()
external_processors = {"TWP", "TWP (US)", "Fin - MID 01", "Npay", "Dreamzpay - Altitudepay"}

# -------------------------
# 🔍 Prediction Logic
# -------------------------
def predict_top_processors(bin_number, is_3d_encoded, top_n=5, threshold=0.80):
    bin_prefix = bin_number // 1000
    bin_suffix = bin_number % 1000

    rows = []
    for processor_id in stats["all_processors"]:
        bin_stats = stats["bin_tx"].get(bin_number, {})
        bin_success = stats["bin_success"].get(bin_number, {}).get("bin_success_rate", 0.0)
        proc_success = stats["proc_success"].get(processor_id, {}).get("processor_success_rate", 0.0)
        bin_proc_stats = stats["bin_proc_stats"].get((bin_number, processor_id), {})

        row = {
            "bin": bin_number,
            "bin_prefix": bin_prefix,
            "bin_suffix": bin_suffix,
            "is_3d_encoded": is_3d_encoded,
            "bin_tx_count": bin_stats.get("bin_tx_count", 0),
            "bin_success_rate": bin_success,
            "processor_success_rate": proc_success,
            "bin_processor_tx_count": bin_proc_stats.get("bin_processor_tx_count", 0),
            "bin_processor_success_count": bin_proc_stats.get("bin_processor_success_count", 0),
            "bin_processor_success_rate": bin_proc_stats.get("bin_processor_success_rate", 0.0)
        }
        rows.append((processor_id, row))

    if not rows:
        return [], False

    df_pred = pd.DataFrame([r[1] for r in rows])
    probs = model.predict_proba(df_pred)[:, 1]

    results = sorted(
        [ {
            "processor": reverse_processor_map.get(r[0], f"Unknown ID {r[0]}"),
            "predicted_success": round(prob * 100, 2)
        } for r, prob in zip(rows, probs)],
        key=lambda x: x["predicted_success"],
        reverse=True
    )

    internal = [r for r in results if r["processor"] not in external_processors and r["predicted_success"] >= threshold * 100]
    external = [r for r in results if r["processor"] in external_processors]
    fallback = False if internal else True
    return (internal if internal else external)[:top_n], fallback

# -------------------------
# 💡 Title Section
# -------------------------
st.markdown("<h1 style='margin-top: 20px;'>BIN-based Processor Success Predictor</h1>", unsafe_allow_html=True)
st.markdown("Use this tool to predict top-performing processors for any BIN using AI-powered success rates.")

# -------------------------
# 📝 Input Section
# -------------------------
with st.container():
    st.markdown("<div style='background: #f9f9f9; padding: 1.5rem; border-radius: 12px;'>", unsafe_allow_html=True)
    input_col1, input_col2 = st.columns([1, 2])

    with input_col1:
        input_method = st.radio("Select Input Method:", ["Manual Entry", "Upload CSV"])

    bin_list = []
    with input_col2:
        if input_method == "Manual Entry":
            bin_input = st.text_input("Enter BINs (comma-separated):", "510123, 462263")
            bin_list = [int(b.strip()) for b in bin_input.split(",") if b.strip().isdigit()]
        else:
            uploaded_file = st.file_uploader("Upload a CSV with a 'BIN' column", type=["csv"])
            if uploaded_file:
                df_uploaded = pd.read_csv(uploaded_file)
                if "BIN" in df_uploaded.columns:
                    bin_list = df_uploaded["BIN"].dropna().astype(int).tolist()
                else:
                    st.error("❌ CSV must contain a 'BIN' column.")

    is_3d = st.selectbox("Is 3D Secure Enabled?", options=[0, 1], index=1)
    st.markdown("</div>", unsafe_allow_html=True)

predict = st.button("Predict Processors")

# -------------------------
# 📊 Results Section
# -------------------------
if predict and bin_list:
    all_results = []
    for bin_no in bin_list:
        top_processors, _ = predict_top_processors(bin_no, is_3d)
        dens_rank = 1
        dens_rank_value = 0
        for rank, proc in enumerate(top_processors, 1):
            fallback_used = "Yes" if proc["processor"] in external_processors else "No"
            if dens_rank_value == proc["predicted_success"]:
                dens_rank+=1
            else:
                dens_rank_value = proc["predicted_success"]
            all_results.append({
                "BIN": bin_no,
                "Processor": proc["processor"],
                "Predicted Success %": proc["predicted_success"],
                "Rank": dens_rank,
                "Fallback External": fallback_used
            })

    if all_results:
        df_result = pd.DataFrame(all_results)
        st.success("✅ Prediction Complete!")
        st.dataframe(df_result, use_container_width=True)

        # ✅ FIX: Group predictions by Processor and average success %
        df_plot = df_result.groupby(["Processor", "Fallback External"], as_index=False)["Predicted Success %"].mean()

        # 📊 Processor Success Distribution (Averaged)
        st.markdown("### 📊 Processor Success Distribution")
        chart = alt.Chart(df_plot).mark_bar().encode(
            x=alt.X("Processor:N", sort="-y"),
            y="Predicted Success %:Q",
            color=alt.Color("Fallback External:N", scale=alt.Scale(scheme='blues')),
            tooltip=["Processor", "Predicted Success %", "Fallback External"]
        ).properties(height=400)
        st.altair_chart(chart, use_container_width=True)

        # 📊 Fallback Count Chart (optional)
        fallback_df = df_result[df_result["Fallback External"] == "Yes"]
        if not fallback_df.empty:
            fallback_counts = fallback_df["Processor"].value_counts().reset_index()
            fallback_counts.columns = ["Processor", "Count"]
            fallback_chart = alt.Chart(fallback_counts).mark_bar().encode(
                x="Processor:N", y="Count:Q"
            ).properties(height=300)
            st.altair_chart(fallback_chart, use_container_width=True)

        # 📋 Summary Stats
        avg_internal = df_result[df_result["Fallback External"] == "No"]["Predicted Success %"].mean()
        avg_external = df_result[df_result["Fallback External"] == "Yes"]["Predicted Success %"].mean()

        avg_internal = 0.0 if pd.isna(avg_internal) else avg_internal
        avg_external = 0.0 if pd.isna(avg_external) else avg_external

        st.markdown("### 🔍 Summary Statistics")
        col1, col2 = st.columns(2)
        col1.metric("Average Internal Processor Success", f"{avg_internal:.2f}%")
        col2.metric("Average External Processor Success", f"{avg_external:.2f}%")

        # 💾 Save and Download
        os.makedirs("logs", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = f"logs/prediction_log_{timestamp}.csv"
        df_result.to_csv(log_path, index=False)
        csv_data = df_result.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download Report as CSV", csv_data, "processor_predictions.csv", "text/csv")
        st.info(f"📝 Log saved as: `{log_path}`")
    else:
        st.error("❌ No predictions could be made.")
