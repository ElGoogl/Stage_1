from control.control_panel_v2 import ControlPanelV2
from control.control_panel_v1 import control_pipeline_step

if __name__ == "__main__":
    # === CONTROL PANEL V1 (Sequential) ===
    # Uncomment the lines below to run V1:
    print("Starting Control Panel V1...")
    for i in range(5):
        print(f"V1 Pipeline step {i+1}/5")
        control_pipeline_step()
    print("✅ v1-Pipeline (5 Schritte) beendet.")
    
    # === CONTROL PANEL V2 (Parallel) ===
    # Uncomment the lines below to run V2:
    print("\nStarting Control Panel V2...")
    cp = ControlPanelV2(
        batch_size=5,          # ← genau 5 Bücher pro Run
        queue_size=4,          # Puffer zwischen Downloader & Indexer
        throttle_seconds=0.5,  # leichtes Drosseln der Downloads
        max_random_tries=200   # genügend Versuche, falls IDs fehlschlagen/dupliziert sind
    )
    cp.start()
    cp.join()
    print("✅ v2-Batch (5 Bücher) beendet.")