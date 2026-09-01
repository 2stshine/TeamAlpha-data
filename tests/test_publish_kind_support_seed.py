from io import BytesIO
from unittest.mock import Mock

from pipeline import publish_kind_support_seed


def test_publish_kind_support_seed_puts_verified_immutable_body(
    monkeypatch,
):
    client = Mock()
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bucket")
    monkeypatch.setattr(
        publish_kind_support_seed.boto3, "client", lambda service: client,
    )

    publish_kind_support_seed.run()

    assert client.put_object.call_count == len(
        publish_kind_support_seed.BODY_SEEDS
    )
    for call in client.put_object.call_args_list:
        values = call.kwargs
        assert values["Bucket"] == "bucket"
        assert values["IfNoneMatch"] == "*"
        assert values["ContentType"] == "text/html"
        assert values["Body"] == publish_kind_support_seed.BODY_SEEDS[
            values["Key"]
        ]


def test_publish_kind_support_seed_accepts_identical_existing_body(
    monkeypatch,
):
    from botocore.exceptions import ClientError

    client = Mock()
    client.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed"}}, "PutObject",
    )
    response_bodies = []

    def get_object(*, Bucket, Key):
        assert Bucket == "bucket"
        body = Mock(wraps=BytesIO(
            publish_kind_support_seed.BODY_SEEDS[Key]
        ))
        response_bodies.append(body)
        return {"Body": body}

    client.get_object.side_effect = get_object
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bucket")
    monkeypatch.setattr(
        publish_kind_support_seed.boto3, "client", lambda service: client,
    )

    publish_kind_support_seed.run()

    assert len(response_bodies) == len(
        publish_kind_support_seed.BODY_SEEDS
    )
    for body in response_bodies:
        body.close.assert_called_once_with()
