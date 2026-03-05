from pathlib import Path

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline


def main() -> None:
    workspace_root = Path(__file__).resolve().parent
    input_dir = workspace_root / "input_queue"
    output_root = workspace_root / "output"
    config = PipelineConfig(
        input_queue_dir=str(input_dir),
        output_root_dir=str(output_root),
        delete_failed_quarantine_dir=str(workspace_root / "output" / "_delete_failed"),
        execute_remote=True,
        accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),),
    )
    result = run_pipeline(config)
    print(result.summary)
    print(f"success={result.success} exit_code={result.exit_code}")
    print(f"run_id={result.run_id}")
    print(f"remote_run_dir={result.remote_run_dir}")
    print(f"local_artifacts_dir={result.local_artifacts_dir}")


if __name__ == "__main__":
    main()
