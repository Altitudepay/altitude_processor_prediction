def predict_processor_from_bin(bin_number, is_3d_encoded=1):
    import pickle
    import pandas as pd
    import json

    # Load model & encoders
    with open("bin_processor_model.pkl", "rb") as f:
        model = pickle.load(f)

    with open("bin_processor_label_encoder.pkl", "rb") as f:
        label_encoder = pickle.load(f)

    with open("bin_processor_stats.pkl", "rb") as f:
        stats = pickle.load(f)

    with open("processor_name_mapping.json", "r") as f:
        proc_name_map = json.load(f)

    reverse_map = {v: k for k, v in proc_name_map.items()}

    bin_prefix = bin_number // 1000
    bin_suffix = bin_number % 1000

    predictions = []
    bin_known = bin_number in stats["bin_tx"]

    for proc in stats["top_processors"]:
        # Extract stats or fall back to 0
        bin_tx_count = stats["bin_tx"].get(bin_number, {}).get("bin_tx_count", 0)
        bin_success_rate = stats["bin_success"].get(bin_number, {}).get("bin_success_rate", 0)
        processor_success_rate = stats["proc_success"].get(proc, {}).get("processor_success_rate", 0)

        bin_proc_key = (bin_number, proc)
        bin_proc_stats = stats["bin_proc_stats"].get(bin_proc_key, {})
        bin_proc_tx = bin_proc_stats.get("bin_processor_tx_count", 0)
        bin_proc_success = bin_proc_stats.get("bin_processor_success_count", 0)
        bin_proc_success_rate = bin_proc_stats.get("bin_processor_success_rate", 0)

        row = pd.DataFrame([{
            "bin": bin_number,
            "bin_prefix": bin_prefix,
            "bin_suffix": bin_suffix,
            "is_3d_encoded": is_3d_encoded,
            "bin_tx_count": bin_tx_count,
            "bin_success_rate": bin_success_rate,
            "processor_success_rate": processor_success_rate,
            "bin_processor_tx_count": bin_proc_tx,
            "bin_processor_success_count": bin_proc_success,
            "bin_processor_success_rate": bin_proc_success_rate
        }])

        try:
            prob = model.predict_proba(row)[0]
            encoded_pred = model.predict(row)[0]
            actual_label = label_encoder.inverse_transform([encoded_pred])[0]
        except Exception as e:
            # fallback prob if prediction fails
            prob = [0.0] * len(label_encoder.classes_)
            actual_label = proc  # default to current processor

        readable_name = reverse_map.get(actual_label, f"Processor {actual_label}")

        predictions.append({
            "processor_encoded": proc,
            "processor_label": readable_name,
            "probability": max(prob),
            "used_fallback": not bin_known
        })

    # Sort and return top 3
    predictions = sorted(predictions, key=lambda x: x["probability"], reverse=True)

    return predictions[:5]  # Return top 3 predictions