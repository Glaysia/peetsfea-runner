from pathlib import Path

from peetsfea_runner import PipelineConfig, run_pipeline


def main() -> None:
    workspace_root = Path(__file__).resolve().parent
    input_dir = workspace_root / "examples"
    config = PipelineConfig(
        input_aedt_dir=str(input_dir),
        execute_remote=True
        )
    result = run_pipeline(config)
    print(result.summary)
    print(f"success={result.success} exit_code={result.exit_code}")
    print(f"run_id={result.run_id}")
    print(f"remote_run_dir={result.remote_run_dir}")
    print(f"local_artifacts_dir={result.local_artifacts_dir}")


if __name__ == "__main__":
    main()
