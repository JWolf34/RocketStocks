"""Tests for rocketstocks.core.config.secrets."""
import importlib
from unittest.mock import patch


def _reload_secrets(env_map: dict):
    """Reload the secrets module patching get_env at its SOURCE (environment module)
    so the re-executed `from environment import get_env` picks up the mock."""
    with patch("rocketstocks.core.config.environment.get_env", side_effect=lambda k: env_map.get(k)):
        import rocketstocks.core.config.secrets as secrets_mod
        importlib.reload(secrets_mod)
        yield secrets_mod


class TestSecrets:
    def test_all_expected_attributes_exist(self):
        """Verify the secrets class exposes all required attributes."""
        import rocketstocks.core.config.secrets as secrets_mod
        required = [
            "discord_token", "news_api_token",
            "schwab_api_key", "schwab_api_secret",
            "db_user", "db_password", "db_name", "db_host", "db_port",
        ]
        for attr in required:
            assert hasattr(secrets_mod.secrets, attr), f"Missing attribute: {attr}"

    def test_discord_token_resolved_from_env(self):
        env_map = {"DISCORD_TOKEN": "tok123"}
        with patch("rocketstocks.core.config.environment.get_env", side_effect=lambda k: env_map.get(k)):
            import rocketstocks.core.config.secrets as secrets_mod
            importlib.reload(secrets_mod)
            assert secrets_mod.secrets.discord_token == "tok123"

    def test_schwab_keys_resolved_from_env(self):
        env_map = {"SCHWAB_API_KEY": "sk", "SCHWAB_API_SECRET": "ss"}
        with patch("rocketstocks.core.config.environment.get_env", side_effect=lambda k: env_map.get(k)):
            import rocketstocks.core.config.secrets as secrets_mod
            importlib.reload(secrets_mod)
            assert secrets_mod.secrets.schwab_api_key == "sk"
            assert secrets_mod.secrets.schwab_api_secret == "ss"

    def test_db_credentials_resolved_from_env(self):
        env = {
            "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "pw",
            "POSTGRES_DB": "db", "POSTGRES_HOST": "host", "POSTGRES_PORT": "5433",
        }
        with patch("rocketstocks.core.config.environment.get_env", side_effect=lambda k: env.get(k)):
            import rocketstocks.core.config.secrets as secrets_mod
            importlib.reload(secrets_mod)
            assert secrets_mod.secrets.db_user == "u"
            assert secrets_mod.secrets.db_host == "host"

    def test_missing_env_var_returns_none(self):
        with patch("rocketstocks.core.config.environment.get_env", return_value=None):
            import rocketstocks.core.config.secrets as secrets_mod
            importlib.reload(secrets_mod)
            assert secrets_mod.secrets.discord_token is None
