"""Wire round-trip for the ENH-011 quota fields + the QUOTA_EXCEEDED error code."""

from par_rt_db.errors import _STATUS, ErrorCode
from par_rt_db.http_client import HotConfig, HotConfigPatch


def test_hot_config_quota_fields_round_trip():
    hot = HotConfig.model_validate(
        {
            "allowedOrigins": [],
            "sessionTtlDays": 30,
            "maxFileSize": 100,
            "idempotencyTtlMs": 300000,
            "maxTablesPerDb": 10,
            "maxStorageBytesPerDb": 1048576,
            "maxSubsPerDb": 50,
        }
    )
    assert hot.max_tables_per_db == 10
    assert hot.max_storage_bytes_per_db == 1048576
    assert hot.max_subs_per_db == 50
    # camelCase alias on the wire
    assert hot.model_dump(by_alias=True)["maxStorageBytesPerDb"] == 1048576


def test_hot_config_quota_fields_default_to_zero_when_absent():
    hot = HotConfig.model_validate(
        {
            "allowedOrigins": [],
            "sessionTtlDays": 30,
            "maxFileSize": 100,
            "idempotencyTtlMs": 300000,
        }
    )
    assert hot.max_tables_per_db == 0
    assert hot.max_storage_bytes_per_db == 0
    assert hot.max_subs_per_db == 0


def test_hot_config_patch_omits_unset_quota_fields():
    patch = HotConfigPatch(max_subs_per_db=5)
    assert patch.model_dump(exclude_none=True, by_alias=True) == {"maxSubsPerDb": 5}


def test_quota_exceeded_code_registered():
    assert ErrorCode.QUOTA_EXCEEDED.value == "QUOTA_EXCEEDED"
    assert _STATUS[ErrorCode.QUOTA_EXCEEDED] == 507
