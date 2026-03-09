"""Tests for data/auth/token_exchange.py."""
import json
import pytest
from unittest.mock import MagicMock, patch


class TestExchangeCodeForToken:
    def _exchange(self, received_url, token_path, auth_context=None, mock_client=None):
        """Helper that calls exchange_code_for_token with mocked schwab."""
        ac = auth_context or MagicMock()
        mc = mock_client or MagicMock()
        with patch("rocketstocks.data.auth.token_exchange.schwab") as mock_schwab:
            mock_schwab.auth.client_from_received_url.return_value = mc
            from rocketstocks.data.auth.token_exchange import exchange_code_for_token
            result = exchange_code_for_token(
                api_key="test_key",
                app_secret="test_secret",
                auth_context=ac,
                received_url=received_url,
                token_path=token_path,
                asyncio=True,
            )
        return result, mock_schwab

    def test_returns_client_on_success(self, tmp_path):
        token_path = str(tmp_path / "token.json")
        mock_client = MagicMock()
        result, _ = self._exchange("https://127.0.0.1:8182/?code=abc&state=xyz", token_path, mock_client=mock_client)
        assert result is mock_client

    def test_calls_client_from_received_url_with_correct_args(self, tmp_path):
        token_path = str(tmp_path / "token.json")
        auth_context = MagicMock()
        received_url = "https://127.0.0.1:8182/?code=abc&state=xyz"
        with patch("rocketstocks.data.auth.token_exchange.schwab") as mock_schwab:
            mock_schwab.auth.client_from_received_url.return_value = MagicMock()
            from rocketstocks.data.auth.token_exchange import exchange_code_for_token
            exchange_code_for_token(
                api_key="k",
                app_secret="s",
                auth_context=auth_context,
                received_url=received_url,
                token_path=token_path,
            )
        call_kwargs = mock_schwab.auth.client_from_received_url.call_args[1]
        assert call_kwargs["api_key"] == "k"
        assert call_kwargs["app_secret"] == "s"
        assert call_kwargs["auth_context"] is auth_context
        assert call_kwargs["received_url"] == received_url
        assert callable(call_kwargs["token_write_func"])

    def test_token_write_func_saves_to_file(self, tmp_path):
        token_path = str(tmp_path / "token.json")
        captured_write_func = None

        def capture_and_call(**kwargs):
            nonlocal captured_write_func
            captured_write_func = kwargs["token_write_func"]
            return MagicMock()

        with patch("rocketstocks.data.auth.token_exchange.schwab") as mock_schwab:
            mock_schwab.auth.client_from_received_url.side_effect = capture_and_call
            from rocketstocks.data.auth.token_exchange import exchange_code_for_token
            exchange_code_for_token(
                api_key="k",
                app_secret="s",
                auth_context=MagicMock(),
                received_url="https://127.0.0.1:8182/?code=abc",
                token_path=token_path,
            )

        assert captured_write_func is not None
        token_data = {"token": {"expires_at": 9999999999}}
        captured_write_func(token_data)
        with open(token_path) as f:
            saved = json.load(f)
        assert saved == token_data

    def test_raises_on_exchange_failure(self, tmp_path):
        token_path = str(tmp_path / "token.json")
        with patch("rocketstocks.data.auth.token_exchange.schwab") as mock_schwab:
            mock_schwab.auth.client_from_received_url.side_effect = ValueError("bad code")
            from rocketstocks.data.auth.token_exchange import exchange_code_for_token
            with pytest.raises(ValueError, match="bad code"):
                exchange_code_for_token(
                    api_key="k",
                    app_secret="s",
                    auth_context=MagicMock(),
                    received_url="https://127.0.0.1:8182/?code=bad",
                    token_path=token_path,
                )
