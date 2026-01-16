"""Tests for SSL certificate helpers."""
import os
import tempfile
import time
import platform
import logging
from pathlib import Path
from datetime import datetime, timezone

import pytest

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

import llm_memedescriber.ssl_helpers
from llm_memedescriber.ssl_helpers import (
    get_or_create_self_signed_cert,
    validate_certificate_files,
    _get_certificate_expiration,
    _is_self_signed_cert,
    _validate_pem_format,
    _validate_cert_key_match,
    CERT_REGENERATION_THRESHOLD_DAYS,
)

from tests._helpers import create_test_cert


class TestGetCertificateExpiration:
    """Tests for certificate expiration reading."""

    def test_get_expiration_from_valid_cert(self):
        """Test reading expiration from a valid certificate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "test.crt")
            key_path = os.path.join(tmpdir, "test.key")
            create_test_cert(cert_path, key_path, days_valid=100)

            expiration = _get_certificate_expiration(cert_path)
            assert expiration is not None
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
            create_test_cert(cert_path, key_path, self_signed=True)

            assert _is_self_signed_cert(cert_path) is True

    def test_non_self_signed_certificate(self):
        """Test detection of non-self-signed certificate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "signed.crt")
            key_path = os.path.join(tmpdir, "signed.key")
            create_test_cert(cert_path, key_path, self_signed=False, issuer_cn="External CA")

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
        cert_path, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)
        original_cert_mtime = os.path.getmtime(cert_path)

        time.sleep(0.1)

        cert_path2, key_path2 = get_or_create_self_signed_cert(cert_dir=cert_dir)

        assert cert_path == cert_path2
        assert key_path == key_path2
        assert os.path.getmtime(cert_path) == original_cert_mtime

    def test_regenerate_expiring_cert(self, tmp_path):
        """Test regeneration of certificate expiring within threshold."""
        cert_dir = str(tmp_path)
        cert_path = os.path.join(cert_dir, "server.crt")
        key_path = os.path.join(cert_dir, "server.key")

        create_test_cert(cert_path, key_path, days_valid=CERT_REGENERATION_THRESHOLD_DAYS - 5)
        original_cert_mtime = os.path.getmtime(cert_path)

        time.sleep(0.1)

        cert_path_result, key_path_result = get_or_create_self_signed_cert(cert_dir)

        assert os.path.getmtime(cert_path) > original_cert_mtime
        assert _is_self_signed_cert(cert_path) is True

    def test_logs_expiration_info(self, tmp_path, caplog):
        """Test that expiration information is logged."""
        cert_dir = str(tmp_path)
        cert_path, key_path = get_or_create_self_signed_cert(cert_dir=cert_dir)

        caplog.set_level(logging.INFO)
        get_or_create_self_signed_cert(cert_dir=cert_dir)

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
            
            with open(cert_path, "rb") as f:
                cert_data = f.read()
            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            
            cn_found = False
            for attr in cert.subject:
                if attr.oid == x509.oid.NameOID.COMMON_NAME and attr.value == hostname:
                    cn_found = True
                    break
            
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
        cert_file, key_file = create_test_cert(
            str(tmp_path / "cert.pem"),
            str(tmp_path / "key.pem"),
            days_valid=365
        )
        
        result_cert, result_key = validate_certificate_files(cert_file, key_file)
        
        assert result_cert == cert_file
        assert result_key == key_file

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
        cert_file, key_file = create_test_cert(
            str(tmp_path / "cert.pem"),
            str(tmp_path / "key.pem"),
            days_valid=365
        )
        
        original_validate = llm_memedescriber.ssl_helpers._validate_pem_format
        
        def mock_validate(path, file_type):
            if path == cert_file:
                raise IOError("Permission denied")
            return original_validate(path, file_type)
        
        monkeypatch.setattr(llm_memedescriber.ssl_helpers, "_validate_pem_format", mock_validate)
        
        with pytest.raises(ValueError, match="Cannot read certificate files|Certificate validation failed"):
            validate_certificate_files(cert_file, key_file)

    def test_cert_path_string_validation(self, tmp_path):
        """Test that cert paths are returned as strings."""
        cert_file, key_file = create_test_cert(
            str(tmp_path / "cert.pem"),
            str(tmp_path / "key.pem"),
            days_valid=365
        )
        
        result_cert, result_key = validate_certificate_files(cert_file, key_file)
        
        assert isinstance(result_cert, str)
        assert isinstance(result_key, str)


class TestIntegrationScenarios:
    """Integration tests for common deployment scenarios."""

    def test_production_with_custom_certs_scenario(self, tmp_path):
        """Test scenario: production with custom certificates via env vars."""
        cert_file, key_file = create_test_cert(
            str(tmp_path / "custom_cert.pem"),
            str(tmp_path / "custom_key.pem"),
            days_valid=365
        )
        
        result_cert, result_key = validate_certificate_files(cert_file, key_file)
        
        assert result_cert == cert_file
        assert result_key == key_file
        with open(result_cert, "rb") as f:
            content = f.read()
            assert b"BEGIN CERTIFICATE" in content

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
            create_test_cert(cert_path, key_path, days_valid=-1)  # Already expired

            with pytest.raises(ValueError, match="EXPIRED"):
                validate_certificate_files(cert_path, key_path)

    def test_warn_user_cert_expiring_within_30_days(self, caplog):
        """Test warning for user cert expiring within 30 days."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "expiring.crt")
            key_path = os.path.join(tmpdir, "expiring.key")
            create_test_cert(cert_path, key_path, days_valid=15)

            caplog.set_level(logging.WARNING)
            result_cert, result_key = validate_certificate_files(cert_path, key_path)

            assert result_cert == cert_path
            assert result_key == key_path
            assert "expires in" in caplog.text

    def test_accept_user_cert_valid_beyond_30_days(self, caplog):
        """Test acceptance of user cert valid beyond 30 days."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "valid.crt")
            key_path = os.path.join(tmpdir, "valid.key")
            create_test_cert(cert_path, key_path, days_valid=365)

            caplog.set_level(logging.INFO)
            result_cert, result_key = validate_certificate_files(cert_path, key_path)

            assert result_cert == cert_path
            assert result_key == key_path
            assert "is valid" in caplog.text

    def test_allows_startup_with_expiring_cert(self):
        """Test that app starts even with cert expiring within 30 days (only warning)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "expiring.crt")
            key_path = os.path.join(tmpdir, "expiring.key")
            create_test_cert(cert_path, key_path, days_valid=10)

            cert, key = validate_certificate_files(cert_path, key_path)
            assert cert == cert_path
            assert key == key_path

    def test_blocks_startup_with_expired_cert(self):
        """Test that app does NOT start with expired cert (raises error)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "expired.crt")
            key_path = os.path.join(tmpdir, "expired.key")
            create_test_cert(cert_path, key_path, days_valid=-10)

            with pytest.raises(ValueError, match="EXPIRED"):
                validate_certificate_files(cert_path, key_path)

    def test_self_signed_cert_check(self):
        """Test that validation correctly identifies self-signed vs CA-signed certs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self_signed_cert = os.path.join(tmpdir, "self_signed.crt")
            self_signed_key = os.path.join(tmpdir, "self_signed.key")
            create_test_cert(self_signed_cert, self_signed_key, days_valid=365, self_signed=True)

            ca_signed_cert = os.path.join(tmpdir, "ca_signed.crt")
            ca_signed_key = os.path.join(tmpdir, "ca_signed.key")
            create_test_cert(ca_signed_cert, ca_signed_key, days_valid=365, self_signed=False, issuer_cn="My CA")

            cert, key = validate_certificate_files(self_signed_cert, self_signed_key)
            assert cert == self_signed_cert

            cert, key = validate_certificate_files(ca_signed_cert, ca_signed_key)
            assert cert == ca_signed_cert


class TestValidatePemFormat:
    """Tests for PEM format validation."""
    
    def test_validate_certificate_valid_pem(self, tmp_path):
        """Test that valid certificate PEM is accepted."""
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path))
        
        result = _validate_pem_format(cert_path, "certificate")
        assert result is True
    
    def test_validate_key_valid_pem(self, tmp_path):
        """Test that valid key PEM is accepted."""
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path))
        
        result = _validate_pem_format(key_path, "key")
        assert result is True
    
    def test_validate_certificate_invalid_pem(self, tmp_path):
        """Test that invalid certificate PEM raises ValueError."""
        invalid_cert = tmp_path / "invalid.crt"
        invalid_cert.write_text("This is not a valid certificate\n")
        
        with pytest.raises(ValueError, match="Invalid PEM format for certificate"):
            _validate_pem_format(str(invalid_cert), "certificate")
    
    def test_validate_key_invalid_pem(self, tmp_path):
        """Test that invalid key PEM raises ValueError."""
        invalid_key = tmp_path / "invalid.key"
        invalid_key.write_text("This is not a valid key\n")
        
        with pytest.raises(ValueError, match="Invalid PEM format for key"):
            _validate_pem_format(str(invalid_key), "key")
    
    def test_validate_nonexistent_file(self, tmp_path):
        """Test that nonexistent file raises ValueError."""
        nonexistent = tmp_path / "does_not_exist.crt"
        
        with pytest.raises(ValueError, match="Invalid PEM format"):
            _validate_pem_format(str(nonexistent), "certificate")


class TestValidateCertKeyMatch:
    """Tests for certificate and key matching."""
    
    def test_matching_cert_and_key(self, tmp_path):
        """Test that matching certificate and key are accepted."""
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path))
        
        # Should not raise
        result = _validate_cert_key_match(cert_path, key_path)
        assert result is True
    
    def test_mismatched_key(self, tmp_path):
        """Test that mismatched key raises ValueError."""
        cert_path1, key_path1 = get_or_create_self_signed_cert(str(tmp_path / "cert1"))
        
        cert_path2, key_path2 = get_or_create_self_signed_cert(str(tmp_path / "cert2"))
        
        with pytest.raises(ValueError, match="Certificate and private key do not match"):
            _validate_cert_key_match(cert_path1, key_path2)
    
    def test_mismatched_cert(self, tmp_path):
        """Test that mismatched cert raises ValueError."""
        cert_path1, key_path1 = get_or_create_self_signed_cert(str(tmp_path / "cert1"))
        
        cert_path2, key_path2 = get_or_create_self_signed_cert(str(tmp_path / "cert2"))
        
        with pytest.raises(ValueError, match="Certificate and private key do not match"):
            _validate_cert_key_match(cert_path2, key_path1)
    
    def test_invalid_cert_format(self, tmp_path):
        """Test that invalid cert format raises ValueError."""
        invalid_cert = tmp_path / "invalid.crt"
        invalid_cert.write_text("This is not a valid certificate\n")
        
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path / "valid"))
        
        with pytest.raises(ValueError, match="Failed to validate certificate/key match|Unable to load PEM file"):
            _validate_cert_key_match(str(invalid_cert), key_path)
    
    def test_invalid_key_format(self, tmp_path):
        """Test that invalid key format raises ValueError."""
        invalid_key = tmp_path / "invalid.key"
        invalid_key.write_text("This is not a valid key\n")
        
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path / "valid"))
        
        with pytest.raises(ValueError, match="Failed to validate certificate/key match|Unable to load PEM file"):
            _validate_cert_key_match(cert_path, str(invalid_key))


class TestIPv6Support:
    """Tests for IPv6 support in SANs."""
    
    def test_ipv6_in_sans(self, tmp_path):
        """Test that IPv6 loopback (::1) is in SANs."""
        cert_path, _ = get_or_create_self_signed_cert(str(tmp_path))
        
        with open(cert_path, "rb") as f:
            cert_data = f.read()
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        san_names = [name for name in san_ext.value]
        
        ipv6_addresses = [name for name in san_names if isinstance(name, x509.IPAddress)]
        ipv6_strs = [str(addr.value) for addr in ipv6_addresses]
        
        assert "::1" in ipv6_strs, f"IPv6 loopback (::1) not found in SANs: {ipv6_strs}"
        assert "127.0.0.1" in ipv6_strs, f"IPv4 loopback not found in SANs: {ipv6_strs}"


class TestKeySize:
    """Tests for configurable RSA key size."""
    
    def test_custom_key_size_2048(self, tmp_path):
        """Test that custom 2048-bit key size is generated."""
        cert_path, _ = get_or_create_self_signed_cert(str(tmp_path), key_size=2048)
        
        with open(cert_path, "rb") as f:
            cert_data = f.read()
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        
        key_size = cert.public_key().key_size
        assert key_size == 2048
    
    def test_custom_key_size_4096(self, tmp_path):
        """Test that custom 4096-bit key size is generated."""
        cert_path, _ = get_or_create_self_signed_cert(str(tmp_path), key_size=4096)
        
        with open(cert_path, "rb") as f:
            cert_data = f.read()
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        
        key_size = cert.public_key().key_size
        assert key_size == 4096


class TestKeyEncryption:
    """Tests for optional key encryption."""
    
    def test_unencrypted_key_by_default(self, tmp_path):
        """Test that private key is unencrypted by default."""
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path))
        
        with open(key_path, "rb") as f:
            key_data = f.read()
        
        key = serialization.load_pem_private_key(key_data, password=None, backend=default_backend())
        assert key is not None
    
    def test_encrypted_key_option(self, tmp_path):
        """Test that key can be encrypted with encrypt_key=True."""
        cert_path, key_path = get_or_create_self_signed_cert(
            str(tmp_path),
            encrypt_key=True
        )
        
        with open(key_path, "rb") as f:
            key_data = f.read()
        
        key = serialization.load_pem_private_key(
            key_data, 
            password=b"development-key-passphrase", 
            backend=default_backend()
        )
        assert key is not None


class TestDirectoryPermissions:
    """Tests for certificate directory permissions."""
    
    def test_cert_dir_permissions_0o700(self, tmp_path):
        """Test that certificate directory has restrictive 0o700 permissions."""
        if os.name != 'posix':
            pytest.skip("File permissions are Unix-specific")
        
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path / "certs"))
        
        cert_dir = tmp_path / "certs"
        
        dir_stat = cert_dir.stat()
        dir_mode = dir_stat.st_mode & 0o777
        
        assert dir_mode == 0o700, f"Expected 0o700, got {oct(dir_mode)}"
    
    def test_key_file_permissions_0o600(self, tmp_path):
        """Test that private key file has restrictive 0o600 permissions."""
        if os.name != 'posix':
            pytest.skip("File permissions are Unix-specific")
        
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path))
        
        key_stat = os.stat(key_path)
        key_mode = key_stat.st_mode & 0o777
        
        assert key_mode == 0o600, f"Expected 0o600, got {oct(key_mode)}"
    
    def test_cert_file_permissions_0o644(self, tmp_path):
        """Test that certificate file has readable 0o644 permissions."""
        if os.name != 'posix':
            pytest.skip("File permissions are Unix-specific")
        
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path))
        
        cert_stat = os.stat(cert_path)
        cert_mode = cert_stat.st_mode & 0o777
        
        assert cert_mode == 0o644, f"Expected 0o644, got {oct(cert_mode)}"


class TestPreciseDayCalculation:
    """Tests for precise day calculation (not truncated to integer)."""
    
    def test_day_calculation_precision(self, tmp_path):
        """Test that day calculation uses precise floating point."""
        cert_path, _ = get_or_create_self_signed_cert(str(tmp_path))
        
        expiration = _get_certificate_expiration(cert_path)
        
        time_delta = expiration - datetime.now(timezone.utc)
        days_until_expiry = time_delta.total_seconds() / 86400
        
        assert 364 < days_until_expiry < 366, f"Expected ~365 days, got {days_until_expiry}"


class TestValidateCertificateFilesWithNewFeatures:
    """Integration tests for validate_certificate_files with new features."""
    
    def test_user_cert_pem_validation(self, tmp_path):
        """Test that user cert PEM format is validated."""
        cert_dir = tmp_path / "user"
        cert_path, key_path = get_or_create_self_signed_cert(str(cert_dir))
        
        result_cert, result_key = validate_certificate_files(cert_path, key_path)
        assert result_cert == cert_path
        assert result_key == key_path
    
    def test_user_cert_key_mismatch_rejected(self, tmp_path):
        """Test that mismatched cert and key are rejected."""
        cert1_dir = tmp_path / "cert1"
        cert2_dir = tmp_path / "cert2"
        
        cert_path1, key_path1 = get_or_create_self_signed_cert(str(cert1_dir))
        cert_path2, key_path2 = get_or_create_self_signed_cert(str(cert2_dir))
        
        with pytest.raises(ValueError, match="Certificate validation failed|Certificate and private key do not match"):
            validate_certificate_files(cert_path1, key_path2)
    
    def test_user_cert_invalid_pem_rejected(self, tmp_path):
        """Test that invalid PEM format is rejected."""
        cert_path = tmp_path / "invalid.crt"
        key_path = tmp_path / "invalid.key"
        
        cert_path.write_text("Not a valid certificate\n")
        key_path.write_text("Not a valid key\n")
        
        with pytest.raises(ValueError, match="Certificate validation failed|Invalid PEM format"):
            validate_certificate_files(str(cert_path), str(key_path))
    
    def test_self_signed_ipv6_support(self, tmp_path):
        """Test that self-signed certs support IPv6."""
        cert_path, key_path = get_or_create_self_signed_cert(str(tmp_path))
        
        with open(cert_path, "rb") as f:
            cert_data = f.read()
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        san_names = [name for name in san_ext.value]
        
        ipv6_addresses = [name for name in san_names if isinstance(name, x509.IPAddress)]
        ipv6_strs = [str(addr.value) for addr in ipv6_addresses]
        
        assert "::1" in ipv6_strs
