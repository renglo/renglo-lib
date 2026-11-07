"""Table names derive from the environment when the config leaves them blank."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import renglo.config_names as config_names
from renglo.config_names import environment_name, table_name


class TableNameTests(unittest.TestCase):
    def setUp(self):
        self._bom = config_names._bom_name_cached
        config_names._bom_name_cached = None

    def tearDown(self):
        config_names._bom_name_cached = self._bom

    def test_explicit_name_wins_over_the_environment(self):
        config = {"WL_NAME": "arbitium0813", "DATA_TABLE": "custom_data", "DYNAMODB_RINGDATA_TABLE": ""}
        self.assertEqual(table_name(config, "DATA_TABLE"), "custom_data")

    def test_legacy_name_wins_over_inference(self):
        config = {"WL_NAME": "arbitium0813", "DYNAMODB_RINGDATA_TABLE": "legacy_data"}
        self.assertEqual(table_name(config, "DATA_TABLE"), "legacy_data")

    def test_blank_tables_use_wl_name(self):
        config = {"WL_NAME": "arbitium0813", "DYNAMODB_RINGDATA_TABLE": ""}
        expected = {
            "DATA_TABLE": "arbitium0813_data",
            "ENTITY_TABLE": "arbitium0813_entities",
            "BLUEPRINT_TABLE": "arbitium0813_blueprints",
            "REL_TABLE": "arbitium0813_rel",
            "CHAT_TABLE": "arbitium0813_chat",
            "GRAPH_TABLE": "arbitium0813_graph",
            "SEARCH_TABLE": "arbitium0813_search",
            "SESSION_TABLE": "arbitium0813_session",
            "TIMESERIES_TABLE": "arbitium0813_timeseries",
        }
        for key, value in expected.items():
            self.assertEqual(table_name(config, key), value)

    def test_renglo_yaml_name_is_used_when_wl_name_is_absent(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            bom = root / "ops" / "acme-bom"
            bom.mkdir(parents=True)
            (bom / "renglo.yaml").write_text("name: acme1\n", encoding="utf-8")
            state = root / "renglo-dev"
            state.mkdir()
            (state / ".renglo-dev.yaml").write_text("bom: Example/acme-bom\n", encoding="utf-8")
            previous = config_names.os.environ.get("RENGLO_WORKSPACE_ROOT")
            wl = config_names.os.environ.pop("WL_NAME", None)
            config_names.os.environ["RENGLO_WORKSPACE_ROOT"] = str(root)
            try:
                self.assertEqual(environment_name({}), "acme1")
                self.assertEqual(table_name({}, "DATA_TABLE"), "acme1_data")
            finally:
                if previous is None:
                    config_names.os.environ.pop("RENGLO_WORKSPACE_ROOT", None)
                else:
                    config_names.os.environ["RENGLO_WORKSPACE_ROOT"] = previous
                if wl is not None:
                    config_names.os.environ["WL_NAME"] = wl

    def test_missing_environment_keeps_the_caller_default(self):
        config_names._bom_name_cached = ""
        wl = config_names.os.environ.pop("WL_NAME", None)
        try:
            self.assertEqual(table_name({}, "ENTITY_TABLE", "default_entity_table"), "default_entity_table")
            self.assertIsNone(table_name({}, "DATA_TABLE"))
        finally:
            if wl is not None:
                config_names.os.environ["WL_NAME"] = wl


if __name__ == "__main__":
    unittest.main()
