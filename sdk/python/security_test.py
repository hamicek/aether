"""Tests for the Python SDK's secured-bus connect options.

They exercise only the pure mapping from injected credentials to nats.connect
kwargs - no running NATS is needed. The CA fixture is a throwaway self-signed
certificate; ssl only parses it here, it is never used for a handshake.
"""

import os
import ssl
import tempfile
import unittest

import aether

_TEST_CA_PEM = b"""-----BEGIN CERTIFICATE-----
MIIBgjCCASegAwIBAgIUI5crPaghb1xl3g8Pa+dwfWIt85wwCgYIKoZIzj0EAwIw
FjEUMBIGA1UEAwwLYWV0aGVyLXRlc3QwHhcNMjYwODA5MTM0NDM0WhcNMzYwODA2
MTM0NDM0WjAWMRQwEgYDVQQDDAthZXRoZXItdGVzdDBZMBMGByqGSM49AgEGCCqG
SM49AwEHA0IABC2xVDtKpdOHu+GMKUW4ld1fEvJ5eXNX9R/DDvUjLy5wnQa7KwsP
HY8Hx7ShCVk6Io7e0a1IT4njpl/Z4N1Lh/WjUzBRMB0GA1UdDgQWBBQ5woUlvzJF
JOfEEkJ+GlLvMjDBajAfBgNVHSMEGDAWgBQ5woUlvzJFJOfEEkJ+GlLvMjDBajAP
BgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0kAMEYCIQDehtj8oktk3PkIvok5
d0OzSZ0VT5xYNHvAnd1lhBdb8QIhANGjypJmuaATmKcOmZxkZudfhC8frcoZqwoW
S0saP6Ra
-----END CERTIFICATE-----
"""


class ConnectKwargsTest(unittest.TestCase):
    def test_unsecured_without_security_block(self):
        kwargs = aether._connect_kwargs("probe")
        self.assertEqual(kwargs, {"name": "probe"})

    def test_nkey_seed_is_passed_as_path(self):
        kwargs = aether._connect_kwargs("probe", nkey_seed="/etc/aether/user.nk")
        self.assertEqual(kwargs["nkeys_seed"], "/etc/aether/user.nk")
        self.assertNotIn("tls", kwargs)

    def test_ca_becomes_an_ssl_context(self):
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as f:
            f.write(_TEST_CA_PEM)
            ca_path = f.name
        try:
            kwargs = aether._connect_kwargs("probe", ca=ca_path)
            self.assertIsInstance(kwargs["tls"], ssl.SSLContext)
            self.assertNotIn("nkeys_seed", kwargs)
        finally:
            os.unlink(ca_path)


if __name__ == "__main__":
    unittest.main()
