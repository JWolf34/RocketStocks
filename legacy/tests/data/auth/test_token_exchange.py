"""Tests for data/auth/token_exchange.py."""
import pytest
from unittest.mock import MagicMock, patch


class TestExchangeCodeForToken:
    def _exchange(self, received_url, auth_context=None, mock_client=None):
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
                asyncio=True,
            )
        return result, mock_schwab

    def test_returns_client_and_token_dict_on_success(self):
        mock_client = MagicMock()
        (client, token_dict), _ = self._exchange(
            "https://127.0.0.1:8182/?code=abc&state=xyz",
            mock_client=mock_client,
        )
        assert client is mock_client
        assert isinstance(token_dict, dict)

    def test_calls_client_from_received_url_with_correct_args(self):
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
            )
        call_kwargs = mock_schwab.auth.client_from_received_url.call_args[1]
        assert call_kwargs["api_key"] == "k"
        assert call_kwargs["app_secret"] == "s"
        assert call_kwargs["auth_context"] is auth_context
        assert call_kwargs["received_url"] == received_url
        assert callable(call_kwargs["token_write_func"])

    def test_captured_token_is_returned(self):
        """The token dict captured by the write func is returned as the second element."""
        token_data = {"creation_timestamp": 1234567890, "token": {"expires_at": 9999}}
        captured_write_func = None

        def capture_and_call(**kwargs):
            nonlocal captured_write_func
            captured_write_func = kwargs["token_write_func"]
            # Simulate schwab-py calling the write func with the token
            captured_write_func(token_data)
            return MagicMock()

        with patch("rocketstocks.data.auth.token_exchange.schwab") as mock_schwab:
            mock_schwab.auth.client_from_received_url.side_effect = capture_and_call
            from rocketstocks.data.auth.token_exchange import exchange_code_for_token
            client, returned_token = exchange_code_for_token(
                api_key="k",
                app_secret="s",
                auth_context=MagicMock(),
                received_url="https://127.0.0.1:8182/?code=abc",
            )

        assert returned_token == token_data

    def test_raises_on_exchange_failure(self):
        with patch("rocketstocks.data.auth.token_exchange.schwab") as mock_schwab:
            mock_schwab.auth.client_from_received_url.side_effect = ValueError("bad code")
            from rocketstocks.data.auth.token_exchange import exchange_code_for_token
            with pytest.raises(ValueError, match="bad code"):
                exchange_code_for_token(
                    api_key="k",
                    app_secret="s",
                    auth_context=MagicMock(),
                    received_url="https://127.0.0.1:8182/?code=bad",
                )
