Run order (Git Bash from project root):
1) bash scripts/01_scrape_odds_example.sh
2) python scripts/02_bronze_to_silver.py --root "C:\\Users\\Ethan\\SGM Agent Project"
3) python scripts/03_models_to_gold.py --root "C:\\Users\\Ethan\\SGM Agent Project" --run_id 20250821_190000
4) bash scripts/04_close_snapshot.sh
