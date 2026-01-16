"""SSL certificate management and self-signed certificate generation."""
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def get_or_create_self_signed_cert(cert_dir: str = "/data/certs", hostname: str = "localhost") -> tuple[str, str]:
    """Get or create a self-signed certificate for development/testing.
    
    Args:
        cert_dir: Directory to store certificates (default: /data/certs)
        hostname: Hostname for the certificate CN (default: localhost)
    
    Returns:
        Tuple of (cert_path, key_path)
    
    Raises:
        RuntimeError: If certificate generation fails
    """
    cert_dir_path = Path(cert_dir)
    cert_path = cert_dir_path / "server.crt"
    key_path = cert_dir_path / "server.key"
    
    if cert_path.exists() and key_path.exists():
        logger.info("Using existing self-signed certificate at %s", cert_path)
        return str(cert_path), str(key_path)
    
    try:
        cert_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise RuntimeError(f"Failed to create certificate directory {cert_dir}: {exc}") from exc
    
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.backends import default_backend
        import datetime
        
        logger.info("Generating self-signed certificate for %s", hostname)
        
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "State"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "City"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "LlmMemeDescriber"),
            x509.NameAttribute(NameOID.COMMON_NAME, hostname),
        ])
        
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.datetime.now(datetime.timezone.utc)
        ).not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365)
        ).add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName(hostname),
                x509.DNSName("*.localhost"),
                x509.IPAddress(__import__('ipaddress').IPv4Address("127.0.0.1")),
            ]),
            critical=False,
        ).sign(private_key, hashes.SHA256(), default_backend())
        
        with open(key_path, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))
        
        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        
        os.chmod(key_path, 0o600)
        os.chmod(cert_path, 0o644)
        
        logger.info("Self-signed certificate generated at %s (valid for 365 days)", cert_path)
        logger.warning("⚠️  Using self-signed certificate. This is only suitable for development/testing.")
        logger.warning("⚠️  For production, provide a proper certificate via SSL_CERT_FILE and SSL_KEY_FILE.")
        
        return str(cert_path), str(key_path)
        
    except ImportError:
        raise RuntimeError(
            "cryptography package required for certificate generation. "
            "Install: pip install cryptography"
        ) from None
    except Exception as exc:
        raise RuntimeError(f"Failed to generate self-signed certificate: {exc}") from exc


def validate_certificate_files(cert_path: str | None, key_path: str | None) -> tuple[str, str]:
    """Validate that certificate and key files exist and are readable.
    
    If neither is provided, generates self-signed certificates.
    If only one is provided, raises an error.
    
    Args:
        cert_path: Path to certificate file (or None)
        key_path: Path to key file (or None)
    
    Returns:
        Tuple of (cert_path, key_path)
    
    Raises:
        ValueError: If configuration is invalid
        RuntimeError: If certificate generation fails
    """
    cert_exists = cert_path and os.path.isfile(cert_path)
    key_exists = key_path and os.path.isfile(key_path)
    
    if cert_exists and key_exists:
        logger.debug("Using provided certificate: %s", cert_path)
        try:
            with open(cert_path, "r") as f:
                f.read()
            with open(key_path, "r") as f:
                f.read()
        except Exception as exc:
            raise ValueError(f"Cannot read certificate files: {exc}") from exc
        return cert_path, key_path
    
    if (cert_path or key_path) and not (cert_exists and key_exists):
        raise ValueError(
            f"SSL certificate configuration incomplete: "
            f"cert_path={cert_path} (exists={cert_exists}), "
            f"key_path={key_path} (exists={key_exists}). "
            f"Both SSL_CERT_FILE and SSL_KEY_FILE must be provided and valid."
        )
    
    logger.info("No SSL certificates provided; generating self-signed certificates")
    return get_or_create_self_signed_cert()
