"""Tests for SSL certificate helpers."""
import os
from pathlib import Path
import pytest
import tempfile
from datetime import datetime, timedelta, timezone

from llm_memedescriber.ssl_helpers import (
    get_or_create_self_signed_cert,
    validate_certificate_files,
    _get_certificate_expiration,
    _is_self_signed_cert,
    CERT_REGENERATION_THRESHOLD_DAYS,
)

try:
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.backends import default_backend
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False


def _create_test_cert(
    cert_path: str,
    key_path: str,
    days_valid: int = 365,
    self_signed: bool = True,
    issuer_cn: str | None = None,
) -> tuple[str, str]:
    """Helper to create a test certificate."""
    if not HAS_CRYPTOGRAPHY:
        pytest.skip("cryptography not installed")
    
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Test"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Test"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test"),
            x509.NameAttribute(NameOID.COMMON_NAME, "test.example.com"),
        ]
    )

    issuer = subject if self_signed else x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_cn or "CA")])

    # For expired certs, set not_valid_before to past and not_valid_after to further past
    if days_valid < 0:
        not_valid_before = datetime.now(timezone.utc) + timedelta(days=days_valid * 2)
        not_valid_after = datetime.now(timezone.utc) + timedelta(days=days_valid)
    else:
        not_valid_before = datetime.now(timezone.utc)
        not_valid_after = datetime.now(timezone.utc) + timedelta(days=days_valid)

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_valid_before)
        .not_valid_after(not_valid_after)
        .sign(private_key, hashes.SHA256(), default_backend())
    )

    Path(cert_path).parent.mkdir(parents=True, exist_ok=True)
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    with open(key_path, "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    return cert_path, key_path


class TestGetCertificateExpiration:
    """Tests for certificate expiration reading."""

    def test_get_expiration_from_valid_cert(self):
        """Test reading expiration from a valid certificate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "test.crt")
            key_path = os.path.join(tmpdir, "test.key")
            _create_test_cert(cert_path, key_path, days_valid=100)

            expiration = _get_certificate_expiration(cert_path)
            assert expiration is not None
            # Should be approximately 100 days from now
            days_diff = (expiration - datetime.now(timezone.utc)).days
            assert 99 <= days_diff <= 101

    def test_get_expiration_from_nonexistent_file(self):
        """Test handling of nonexistent certificate file."""
        expiration = _get_certificate_expiration("/nonexistent/path/cert.pem")
        assert expiration is None

    def test_get_expiration_from_invalid_cert(self):
        """Test handling of invalid certificate file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "invalid.crt")
            with open(cert_path, "w") as f:
                f.write("not a valid certificate")

            expiration = _get_certificate_expiration(cert_path)
            assert expiration is None


class TestIsSelfSignedCert:
    """Tests for self-signed certificate detection."""

    def test_self_signed_certificate(self):
        """Test detection of self-signed certificate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "self_signed.crt")
            key_path = os.path.join(tmpdir, "self_signed.key")
            _create_test_cert(cert_path, key_path, self_signed=True)

            assert _is_self_signed_cert(cert_path) is True

    def test_non_self_signed_certificate(self):
        """Test detection of non-self-signed certificate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "signed.crt")
            key_path = os.path.join(tmpdir, "signed.key")
            _create_test_cert(cert_path, key_path, self_signed=False, issuer_cn="External CA")

            assert _is_self_signed_cert(cert_path) is False

    def test_invalid_cert_file(self):
        """Test handling of invalid certificate file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "invalid.crt")
            with open(cert_path, "w") as f:
                f.write("not a certificate")

            assert _is_self_signed_cert(cert_path) is False


class TestGetOrCreateSelfSignedCertExpiration:
    """Tests for self-signed certificate generation with expiration handling."""

    def test_reuse_existing_valid_cert(self, tmp_path):
        """Test reusing existing valid certificate."""
        cert_dir = str(tmp_path / "certs")
        # Create initial cert
        cert_path, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)
        original_cert_mtime = os.path.getmtime(cert_path)

        # Wait a bit and call again
        import time
        time.sleep(0.1)

        cert_path2, key_path2 = get_or_create_self_signed_cert(cert_dir=cert_dir)

        # Should reuse the same certificate
        assert cert_path == cert_path2
        assert key_path == key_path2
        assert os.path.getmtime(cert_path) == original_cert_mtime

    def test_regenerate_expiring_cert(self, tmp_path):
        """Test regeneration of certificate expiring within threshold."""
        cert_dir = str(tmp_path)
        cert_path = os.path.join(cert_dir, "server.crt")
        key_path = os.path.join(cert_dir, "server.key")

        # Create a cert that expires within regeneration threshold
        _create_test_cert(cert_path, key_path, days_valid=CERT_REGENERATION_THRESHOLD_DAYS - 5)
        original_cert_mtime = os.path.getmtime(cert_path)

        # Wait a bit and call get_or_create again
        import time
        time.sleep(0.1)

        cert_path_result, key_path_result = get_or_create_self_signed_cert(cert_dir)

        # Should regenerate the certificate
        assert os.path.getmtime(cert_path) > original_cert_mtime
        assert _is_self_signed_cert(cert_path) is True

    def test_logs_expiration_info(self, tmp_path, caplog):
        """Test that expiration information is logged."""
        cert_dir = str(tmp_path)
        cert_path, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)

        # Call again to trigger the expiration check
        import logging
        caplog.set_level(logging.INFO)
        get_or_create_self_signed_cert(cert_dir=cert_dir)

        # Should log that cert expires in X days
        assert "expires in" in caplog.text



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


class TestValidateCertificateExpiration:
    """Tests for certificate expiration validation."""

    def test_reject_expired_user_cert(self):
        """Test rejection of expired user-provided certificate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "expired.crt")
            key_path = os.path.join(tmpdir, "expired.key")
            _create_test_cert(cert_path, key_path, days_valid=-1)  # Already expired

            with pytest.raises(ValueError, match="EXPIRED"):
                validate_certificate_files(cert_path, key_path)

    def test_warn_user_cert_expiring_within_30_days(self, caplog):
        """Test warning for user cert expiring within 30 days."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "expiring.crt")
            key_path = os.path.join(tmpdir, "expiring.key")
            _create_test_cert(cert_path, key_path, days_valid=15)

            # Should not raise, but should log warning
            import logging
            caplog.set_level(logging.WARNING)
            result_cert, result_key = validate_certificate_files(cert_path, key_path)

            assert result_cert == cert_path
            assert result_key == key_path
            # Should have warning about expiration
            assert "expires in" in caplog.text

    def test_accept_user_cert_valid_beyond_30_days(self, caplog):
        """Test acceptance of user cert valid beyond 30 days."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "valid.crt")
            key_path = os.path.join(tmpdir, "valid.key")
            _create_test_cert(cert_path, key_path, days_valid=365)

            import logging
            caplog.set_level(logging.INFO)
            result_cert, result_key = validate_certificate_files(cert_path, key_path)

            assert result_cert == cert_path
            assert result_key == key_path
            # Should log success
            assert "is valid" in caplog.text

    def test_allows_startup_with_expiring_cert(self):
        """Test that app starts even with cert expiring within 30 days (only warning)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "expiring.crt")
            key_path = os.path.join(tmpdir, "expiring.key")
            _create_test_cert(cert_path, key_path, days_valid=10)

            # Should NOT raise ValueError
            cert, key = validate_certificate_files(cert_path, key_path)
            
            # But should return the paths
            assert cert == cert_path
            assert key == key_path

    def test_blocks_startup_with_expired_cert(self):
        """Test that app does NOT start with expired cert (raises error)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "expired.crt")
            key_path = os.path.join(tmpdir, "expired.key")
            _create_test_cert(cert_path, key_path, days_valid=-10)  # Expired 10 days ago

            # Should raise ValueError
            with pytest.raises(ValueError, match="EXPIRED"):
                validate_certificate_files(cert_path, key_path)

    def test_self_signed_cert_check(self):
        """Test that validation correctly identifies self-signed vs CA-signed certs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Self-signed
            self_signed_cert = os.path.join(tmpdir, "self_signed.crt")
            self_signed_key = os.path.join(tmpdir, "self_signed.key")
            _create_test_cert(self_signed_cert, self_signed_key, days_valid=365, self_signed=True)

            # CA-signed
            ca_signed_cert = os.path.join(tmpdir, "ca_signed.crt")
            ca_signed_key = os.path.join(tmpdir, "ca_signed.key")
            _create_test_cert(ca_signed_cert, ca_signed_key, days_valid=365, self_signed=False, issuer_cn="My CA")

            # Both should validate if not expired
            cert, key = validate_certificate_files(self_signed_cert, self_signed_key)
            assert cert == self_signed_cert

            cert, key = validate_certificate_files(ca_signed_cert, ca_signed_key)
            assert cert == ca_signed_cert
