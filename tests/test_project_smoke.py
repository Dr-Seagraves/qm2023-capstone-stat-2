import csv
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class TestProjectSmoke(unittest.TestCase):
    def test_required_docs_exist(self):
        required = [
            ROOT / "README.md",
            ROOT / "README(2).md",
            ROOT / "README(3).md",
            ROOT / "M1_data_quality_report.md",
            ROOT / "M2_EDA_summary.md",
            ROOT / "M3_interpretation.md",
            ROOT / "AI_AUDIT_APPENDIX.md",
            ROOT / "requirements.txt",
        ]
        for path in required:
            self.assertTrue(path.exists(), f"Missing required file: {path}")

    def test_required_m3_outputs_exist(self):
        required_tables = [
            ROOT / "results/tables/M3_modelA_breusch_pagan.csv",
            ROOT / "results/tables/M3_modelA_vif.csv",
            ROOT / "results/tables/M3_modelA_regression_table.csv",
            ROOT / "results/tables/M3_modelA_robustness_lags.csv",
            ROOT / "results/tables/M3_modelA_robustness_outlier_trim.csv",
            ROOT / "results/tables/M3_modelA_robustness_group_subsamples.csv",
            ROOT / "results/tables/M3_modelB_option3_metrics.csv",
            ROOT / "results/tables/M3_modelB_option3_feature_importance.csv",
            ROOT / "results/tables/M3_modelB_option3_ols_coefficients.csv",
            ROOT / "results/tables/M3_modelB_option3_predictions.csv",
            ROOT / "results/tables/M3_regression_table.csv",
        ]
        required_figures = [
            ROOT / "results/figures/M3_residuals_vs_fitted.png",
            ROOT / "results/figures/M3_qq_plot.png",
            ROOT / "results/figures/M3_modelB_option3_actual_vs_predicted.png",
        ]

        for path in [*required_tables, *required_figures]:
            self.assertTrue(path.exists(), f"Missing required M3 output: {path}")

    def test_group_subsample_table_contains_all_groups(self):
        table_path = ROOT / "results/tables/M3_modelA_robustness_group_subsamples.csv"
        self.assertTrue(table_path.exists(), f"Missing table: {table_path}")

        with table_path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        groups = {row["token_group"] for row in rows}
        self.assertEqual(groups, {"centralized_exchange", "defi", "stablecoin"})

        for row in rows:
            self.assertNotEqual(row.get("coef", ""), "", f"Missing coefficient for group: {row['token_group']}")


if __name__ == "__main__":
    unittest.main()
