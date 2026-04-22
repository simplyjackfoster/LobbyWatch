import logging
import os
from functools import lru_cache

logger = logging.getLogger(__name__)


def _param_to_env_key(param_name: str) -> str:
    key = (param_name or "").split("/")[-1]
    return key.upper()


@lru_cache(maxsize=1)
def _ssm_client():
    import boto3

    return boto3.client("ssm")


def _get_parameter(name: str) -> str | None:
    if not name:
        return None
    try:
        result = _ssm_client().get_parameter(Name=name, WithDecryption=True)
    except Exception as exc:
        logger.warning("failed to fetch ssm parameter %s: %s", name, exc)
        return None
    return ((result.get("Parameter") or {}).get("Value") or "").strip() or None


def _load_param_prefix(prefix: str):
    if not prefix:
        return
    next_token = None
    while True:
        kwargs = {
            "Path": prefix,
            "WithDecryption": True,
            "Recursive": True,
            "MaxResults": 10,
        }
        if next_token:
            kwargs["NextToken"] = next_token
        payload = _ssm_client().get_parameters_by_path(**kwargs)
        for param in payload.get("Parameters", []):
            name = param.get("Name") or ""
            value = param.get("Value")
            if not value:
                continue
            env_key = _param_to_env_key(name)
            os.environ.setdefault(env_key, value)
        next_token = payload.get("NextToken")
        if not next_token:
            break


_bootstrapped = False


def bootstrap_ssm_env():
    global _bootstrapped
    if _bootstrapped:
        return

    if os.getenv("ENABLE_SSM_CONFIG") != "1":
        _bootstrapped = True
        return

    prefix = os.getenv("SSM_PARAM_PREFIX", "").strip()
    if prefix:
        try:
            _load_param_prefix(prefix)
        except Exception as exc:
            logger.warning("failed to load ssm path prefix %s: %s", prefix, exc)

    db_param = os.getenv("DATABASE_URL_PARAM", "").strip()
    if db_param and not os.getenv("DATABASE_URL"):
        value = _get_parameter(db_param)
        if value:
            os.environ["DATABASE_URL"] = value

    secret_param = os.getenv("CF_API_SHARED_SECRET_PARAM", "").strip()
    if secret_param and not os.getenv("CF_API_SHARED_SECRET"):
        value = _get_parameter(secret_param)
        if value:
            os.environ["CF_API_SHARED_SECRET"] = value

    _bootstrapped = True
