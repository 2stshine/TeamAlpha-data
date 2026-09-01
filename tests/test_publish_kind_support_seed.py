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

    client.put_object.assert_called_once()
    call = client.put_object.call_args.kwargs
    assert call["Bucket"] == "bucket"
    assert call["IfNoneMatch"] == "*"
    assert call["ContentType"] == "text/html"
    assert len(call["Body"]) == 3719


def test_publish_kind_support_seed_accepts_identical_existing_body(
    monkeypatch,
):
    from botocore.exceptions import ClientError

    payload = next(iter(publish_kind_support_seed.BODY_SEEDS.values()))
    client = Mock()
    client.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed"}}, "PutObject",
    )
    response_body = Mock(wraps=BytesIO(payload))
    client.get_object.return_value = {"Body": response_body}
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bucket")
    monkeypatch.setattr(
        publish_kind_support_seed.boto3, "client", lambda service: client,
    )

    publish_kind_support_seed.run()

    response_body.close.assert_called_once_with()
