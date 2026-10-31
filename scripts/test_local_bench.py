import unittest

from scripts import local_bench


class ParseCommandTests(unittest.TestCase):
    def test_benchmark_command_forms_keep_their_plan(self) -> None:
        self.assertEqual(
            local_bench.parse_command(["run", "benchmarks", "tpch_mem"], False, None),
            local_bench.Plan(benches=["tpch_mem"]),
        )
        self.assertEqual(
            local_bench.parse_command(
                ["./bench.sh", "run", "tpch_mem", "5"], False, None
            ),
            local_bench.Plan(benches=["tpch_mem"], query="5"),
        )
        self.assertEqual(
            local_bench.parse_command(
                ["cargo", "bench", "--bench", "sql_planner"], False, None
            ),
            local_bench.Plan(raw="cargo bench --bench sql_planner"),
        )


class ProgressStepTests(unittest.TestCase):
    def test_step_count_matches_each_execution_path(self) -> None:
        cases = (
            (local_bench.Plan(raw="echo run"), {}, False, 3),
            (local_bench.Plan(benches=["tpch"]), {}, False, 6),
            (local_bench.Plan(benches=["tpch"]), {}, True, 5),
            (
                local_bench.Plan(benches=["sql_planner"]),
                {"sql_planner": ["parquet"]},
                False,
                4,
            ),
            (local_bench.Plan(benches=["h2o"]), {}, False, 7),
        )
        for plan, targets, skip_data, expected in cases:
            with self.subTest(plan=plan, skip_data=skip_data):
                self.assertEqual(
                    local_bench.count_steps(plan, targets, skip_data), expected
                )


if __name__ == "__main__":
    unittest.main()
