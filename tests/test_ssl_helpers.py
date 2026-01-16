"""Tests for SSL certificate helpers."""
import os
from pathlib import Path
import pytest

from llm_memedescriber.ssl_helpers import (
    get_or_create_self_signed_cert,
    validate_certificate_files,
)


class TestGetOrCreateSelfSignedCert:
    """Tests for self-signed certificate generation."""

    def test_generates_cert_when_missing(self, tmp_path):
        """Test that certificates are generated when not present."""
        cert_dir = str(tmp_path / "certs")
        cert_path, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)

        assert os.path.isfile(cert_path)
        assert os.path.isfile(key_path)
        assert cert_path.endswith("server.crt")
        assert key_path.endswith("server.key")

    def test_generated_cert_is_valid(self, tmp_path):
        """Test that generated certificate has valid content."""
        cert_dir = str(tmp_path / "certs")
        cert_path, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)

        with open(cert_path, "r") as f:
            cert_content = f.read()
        assert "-----BEGIN CERTIFICATE-----" in cert_content
        assert "-----END CERTIFICATE-----" in cert_content

        with open(key_path, "r") as f:
            key_content = f.read()
        assert "-----BEGIN RSA PRIVATE KEY-----" in key_content or "-----BEGIN PRIVATE KEY-----" in key_content
        assert "-----END" in key_content

    def test_reuses_existing_cert(self, tmp_path):
        """Test that existing certificates are reused."""
        cert_dir = str(tmp_path / "certs")
        
        cert_path_1, key_path_1 = get_or_create_self_signed_cert(cert_dir=cert_dir)
        with open(cert_path_1, "r") as f:
            cert_content_1 = f.read()
        
        cert_path_2, key_path_2 = get_or_create_self_signed_cert(cert_dir=cert_dir)
        with open(cert_path_2, "r") as f:
            cert_content_2 = f.read()
        
        assert cert_path_1 == cert_path_2
        assert key_path_1 == key_path_2
        assert cert_content_1 == cert_content_2

    def test_custom_hostname_in_cert(self, tmp_path):
        """Test that custom hostname is used in certificate CN."""
        cert_dir = str(tmp_path / "certs")
        hostname = "example.com"
        cert_path, _ = get_or_create_self_signed_cert(cert_dir=cert_dir, hostname=hostname)

        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            
            with open(cert_path, "rb") as f:
                cert_data = f.read()
            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            
            # Check CN in subject
            cn_found = False
            for attr in cert.subject:
                if attr.oid == x509.oid.NameOID.COMMON_NAME and attr.value == hostname:
                    cn_found = True
                    break
            
            # Check SAN (Subject Alternative Names)
            san_found = False
            try:
                san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
                for name in san_ext.value:
                    if isinstance(name, x509.DNSName) and name.value == hostname:
                        san_found = True
                        break
            except x509.ExtensionNotFound:
                pass
            
            assert cn_found or san_found, f"Hostname {hostname} not found in certificate CN or SAN"
        except ImportError:
            pytest.skip("cryptography not installed")

    def test_key_file_permissions(self, tmp_path):
        """Test that private key has restrictive permissions."""
        import platform
        if platform.system() == "Windows":
            pytest.skip("File permissions work differently on Windows")
        
        cert_dir = str(tmp_path / "certs")
        _, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)

        stat_info = os.stat(key_path)
        mode = stat_info.st_mode & 0o777
        assert mode == 0o600, f"Expected 0o600 but got {oct(mode)}"

    def test_cert_dir_created_if_missing(self, tmp_path):
        """Test that certificate directory is created if it doesn't exist."""
        cert_dir = str(tmp_path / "deep" / "nested" / "certs")
        assert not os.path.exists(cert_dir)
        
        cert_path, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)
        
        assert os.path.isdir(cert_dir)
        assert os.path.isfile(cert_path)
        assert os.path.isfile(key_path)

    def test_cert_valid_for_365_days(self, tmp_path):
        """Test that certificate is valid for approximately 365 days."""
        cert_dir = str(tmp_path / "certs")
        cert_path, _ = get_or_create_self_signed_cert(cert_dir=cert_dir)

        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            
            with open(cert_path, "rb") as f:
                cert_data = f.read()
            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            
            validity_days = (cert.not_valid_after_utc - cert.not_valid_before_utc).days
            assert 364 <= validity_days <= 366, f"Expected ~365 days, got {validity_days}"
        except ImportError:
            pytest.skip("cryptography not installed")


class TestValidateCertificateFiles:
    """Tests for certificate validation and fallback."""

    def test_both_certs_provided_and_valid(self, tmp_path):
        """Test with both certificate files provided and valid."""
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
        key_file.write_text("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----")
        
        result_cert, result_key = validate_certificate_files(
            str(cert_file), str(key_file)
        )
        
        assert result_cert == str(cert_file)
        assert result_key == str(key_file)

    def test_no_certs_provided_generates_self_signed(self, tmp_path, monkeypatch):
        """Test that self-signed certs are generated when none provided."""
        def mock_generate(cert_dir="/data/certs", hostname="localhost"):
            cert_path = tmp_path / "server.crt"
            key_path = tmp_path / "server.key"
            cert_path.write_text("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
            key_path.write_text("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----")
            return str(cert_path), str(key_path)
        
        monkeypatch.setattr(
            "llm_memedescriber.ssl_helpers.get_or_create_self_signed_cert",
            mock_generate
        )
        
        result_cert, result_key = validate_certificate_files(None, None)
        
        assert os.path.isfile(result_cert)
        assert os.path.isfile(result_key)

    def test_cert_provided_but_key_missing_raises_error(self, tmp_path):
        """Test that error is raised if only cert is provided."""
        cert_file = tmp_path / "cert.pem"
        cert_file.write_text("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
        
        with pytest.raises(ValueError, match="SSL certificate configuration incomplete"):
            validate_certificate_files(str(cert_file), None)

    def test_key_provided_but_cert_missing_raises_error(self, tmp_path):
        """Test that error is raised if only key is provided."""
        key_file = tmp_path / "key.pem"
        key_file.write_text("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----")
        
        with pytest.raises(ValueError, match="SSL certificate configuration incomplete"):
            validate_certificate_files(None, str(key_file))

    def test_cert_file_not_found_raises_error(self):
        """Test that error is raised if cert file doesn't exist."""
        with pytest.raises(ValueError, match="SSL certificate configuration incomplete"):
            validate_certificate_files("/nonexistent/cert.pem", "/nonexistent/key.pem")

    def test_both_certs_provided_but_cert_missing_on_disk(self, tmp_path):
        """Test error when cert path is provided but file doesn't exist."""
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        key_file.write_text("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----")
        
        with pytest.raises(ValueError, match="SSL certificate configuration incomplete"):
            validate_certificate_files(str(cert_file), str(key_file))

    def test_cert_file_read_error_raises_error(self, tmp_path, monkeypatch):
        """Test that error is raised if certificate file cannot be read."""
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
        key_file.write_text("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----")
        
        # Mock open to raise an exception when reading cert file
        original_open = open
        def mock_open(path, *args, **kwargs):
            if str(path) == str(cert_file) and 'r' in str(args):
                raise IOError("Permission denied")
            return original_open(path, *args, **kwargs)
        
        monkeypatch.setattr("builtins.open", mock_open)
        
        with pytest.raises(ValueError, match="Cannot read certificate files"):
            validate_certificate_files(str(cert_file), str(key_file))

    def test_cert_path_string_validation(self, tmp_path):
        """Test that cert paths are returned as strings."""
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
        key_file.write_text("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----")
        
        result_cert, result_key = validate_certificate_files(
            str(cert_file), str(key_file)
        )
        
        assert isinstance(result_cert, str)
        assert isinstance(result_key, str)


class TestIntegrationScenarios:
    """Integration tests for common deployment scenarios."""

    def test_production_with_custom_certs_scenario(self, tmp_path):
        """Test scenario: production with custom certificates via env vars."""
        cert_file = tmp_path / "custom_cert.pem"
        key_file = tmp_path / "custom_key.pem"
        
        cert_file.write_text("-----BEGIN CERTIFICATE-----\nproduction\n-----END CERTIFICATE-----")
        key_file.write_text("-----BEGIN RSA PRIVATE KEY-----\nproduction\n-----END RSA PRIVATE KEY-----")
        
        result_cert, result_key = validate_certificate_files(
            str(cert_file), str(key_file)
        )
        
        assert result_cert == str(cert_file)
        assert result_key == str(key_file)
        with open(result_cert) as f:
            assert "production" in f.read()

    def test_default_cert_paths(self, tmp_path, monkeypatch):
        """Test that default cert paths are used correctly."""
        default_cert_dir = str(tmp_path / "data" / "certs")
        
        def mock_generate(cert_dir="/data/certs", hostname="localhost"):
            Path(cert_dir).mkdir(parents=True, exist_ok=True)
            cert_path = Path(cert_dir) / "server.crt"
            key_path = Path(cert_dir) / "server.key"
            cert_path.write_text("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
            key_path.write_text("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----")
            return str(cert_path), str(key_path)
        
        monkeypatch.setattr(
            "llm_memedescriber.ssl_helpers.get_or_create_self_signed_cert",
            mock_generate
        )
        
        result_cert, result_key = validate_certificate_files(None, None)
        
        assert "server.crt" in result_cert
        assert "server.key" in result_key

    def test_https_only_configured(self):
        """Verify app is configured for HTTPS only."""
        
        cert, key = validate_certificate_files(None, None)
        assert os.path.isfile(cert), "Certificate file should exist"
        assert os.path.isfile(key), "Private key file should exist"
        assert "server.crt" in cert, "Should use standard cert filename"
        assert "server.key" in key, "Should use standard key filename"