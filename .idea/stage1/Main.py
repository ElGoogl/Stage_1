from control.control_panel_v2 import ControlPanelV2

if __name__ == "__main__":
    # Starte genau einen Durchlauf mit 5 Büchern (zufällige IDs),
    # Download & Indexierung laufen parallel, State-Dateien werden gepflegt.
    cp = ControlPanelV2(
        batch_size=5,          # ← genau 5 Bücher pro Run
        queue_size=4,          # Puffer zwischen Downloader & Indexer
        throttle_seconds=0.5,  # leichtes Drosseln der Downloads
        max_random_tries=200   # genügend Versuche, falls IDs fehlschlagen/dupliziert sind
    )
    cp.start()
    cp.join()
    print("✅ v2-Batch (5 Bücher) beendet.")