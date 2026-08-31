from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_production_image_is_bound_to_a_full_source_commit():
    dockerfile = _read("deploy/Dockerfile")
    workflow = _read(".github/workflows/deploy.yml")

    assert "ARG SOURCE_COMMIT" in dockerfile
    assert "^[0-9a-f]{40}$" in dockerfile
    assert 'org.opencontainers.image.revision="${SOURCE_COMMIT}"' in dockerfile
    assert 'org.opencontainers.image.source="${SOURCE_REPOSITORY}"' in dockerfile

    assert '--build-arg "SOURCE_COMMIT=${GITHUB_SHA}"' in workflow
    assert (
        '--build-arg "SOURCE_REPOSITORY=${GITHUB_SERVER_URL}/'
        '${GITHUB_REPOSITORY}"'
    ) in workflow


def test_ecs_task_definition_uses_the_pushed_image_digest():
    workflow = _read(".github/workflows/deploy.yml")

    assert "--metadata-file image-metadata.json" in workflow
    assert '\'."containerimage.digest" // empty\'' in workflow
    assert "^sha256:[0-9a-f]{64}$" in workflow
    assert 'IMAGE_URI="${ECR_REGISTRY}/${ECR_REPOSITORY}@${IMAGE_DIGEST}"' in workflow
    assert "IMAGE_URI: ${{ steps.image.outputs.image_uri }}" in workflow
    assert '--arg image "${IMAGE_URI}"' in workflow
    assert '.entryPoint = ["uv", "run", "python", "-m", "pipeline.daily_full"]' in workflow
    assert ".stopTimeout = 120" in workflow
    assert "DAILY_CPU: '8192'" in workflow
    assert "DAILY_MEMORY: '49152'" in workflow
    assert "DAILY_EPHEMERAL_GIB: '120'" in workflow
    assert ".cpu = $task_cpu" in workflow
    assert ".memory = $task_memory" in workflow
    assert '.ephemeralStorage = {"sizeInGiB": $ephemeral_gib}' in workflow
    assert "del(.cpu, .memory, .memoryReservation)" in workflow
    assert ".ephemeralStorage.sizeInGiB == $ephemeral" in workflow


def test_deploy_reactivates_daily_scheduler_after_target_update():
    workflow = _read(".github/workflows/deploy.yml")
    preflight = workflow.split(
        "- name: Inspect current daily Scheduler", 1
    )[1].split("- name: Log in to Amazon ECR", 1)[0]
    update_step = workflow.split(
        "- name: Update EventBridge Scheduler target", 1
    )[1].split("- name: Show deployed version", 1)[0]

    assert "aws scheduler get-schedule" in preflight
    assert '== "ENABLED"' in preflight
    assert '== "DISABLED"' in preflight
    assert "Unexpected daily Scheduler state" in preflight
    assert "aws scheduler get-schedule" in update_step
    assert '--group-name "${SCHEDULE_GROUP}"' in update_step
    assert "--state ENABLED" in update_step
    assert "--state DISABLED" not in update_step
    assert ".EcsParameters.TaskCount = 1" in update_step
    assert '.RetryPolicy = {' in update_step
    assert '"MaximumRetryAttempts": 0' in update_step
    assert ".EcsParameters.TaskCount == 1" in update_step
    assert ".RetryPolicy.MaximumRetryAttempts == 0" in update_step
    assert "// 0" not in update_step


def test_any_mutable_upstream_base_image_risk_is_explicitly_documented():
    dockerfile = _read("deploy/Dockerfile")
    readme = _read("README.md")

    upstream_images = [
        line.split()[1]
        for line in dockerfile.splitlines()
        if line.startswith("FROM ")
    ] + [
        line.split("--from=", 1)[1].split()[0]
        for line in dockerfile.splitlines()
        if line.startswith("COPY --from=")
    ]
    mutable_images = [
        image for image in upstream_images if "@sha256:" not in image
    ]

    # Mutable publisher references must not be presented as reproducibly
    # pinned. This condition becomes unnecessary once independently verified
    # publisher digests replace every mutable reference.
    if mutable_images:
        assert "upstream 참조는 mutable" in readme
        assert "bit-for-bit 동일한 digest" in readme


def test_local_dart_recovery_commands_bind_exact_seed_coverage():
    readme = _read("README.md")

    for module in (
        "pipeline.bronze.dart_viewer_corrections",
        "pipeline.bronze.dart_support_action_families",
    ):
        command = readme.split(f"uv run python -m {module}", 1)[1].split(
            "\n\n", 1,
        )[0]
        assert "--coverage-start 2015-01-01" in command
        assert "--coverage-end 2026-08-10" in command
        assert "--apply" in command
