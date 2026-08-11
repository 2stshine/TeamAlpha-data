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


def test_deploy_preserves_existing_scheduler_state():
    workflow = _read(".github/workflows/deploy.yml")
    update_step = workflow.split(
        "- name: Update EventBridge Scheduler target", 1
    )[1].split("- name: Show deployed version", 1)[0]

    assert "aws scheduler get-schedule" in update_step
    assert "CURRENT_SCHEDULE_STATE" in update_step
    assert "^(ENABLED|DISABLED)$" in update_step
    assert '--state "${CURRENT_SCHEDULE_STATE}"' in update_step
    assert '--group-name "${SCHEDULE_GROUP}"' in update_step
    assert "--state ENABLED" not in update_step


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
