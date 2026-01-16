"""SSL certificate management and self-signed certificate generation."""
import os
import logging
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from llm_memedescriber.constants import CERT_REGENERATION_THRESHOLD_DAYS

logger = logging.getLogger(__name__)



def _validate_pem_format(file_path: str, file_type: str) -> bool:
    """Validate that a file is in valid PEM format.
    
    Args:
        file_path: Path to the file
        file_type: Either 'certificate' or 'key'
    
    Returns:
        True if valid PEM format
    
    Raises:
        ValueError: If file is not in valid PEM format
    """
    try:
        with open(file_path, "rb") as f:
            pem_data = f.read()
        
        if file_type == "certificate":
            x509.load_pem_x509_certificate(pem_data, default_backend())
        elif file_type == "key":
            serialization.load_pem_private_key(pem_data, password=None, backend=default_backend())
        return True
    except Exception as exc:
        raise ValueError(f"Invalid PEM format for {file_type} ({file_path}): {exc}") from exc


def _validate_cert_key_match(cert_path: str, key_path: str) -> bool:
    """Validate that certificate and private key match.
    
    Args:
        cert_path: Path to certificate file
        key_path: Path to private key file
    
    Returns:
        True if they match
    
    Raises:
        ValueError: If certificate and key don't match
    """
    try:
        with open(cert_path, "rb") as f:
            cert_data = f.read()
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        
        with open(key_path, "rb") as f:
            key_data = f.read()
        key = serialization.load_pem_private_key(key_data, password=None, backend=default_backend())
        
        cert_public_numbers = cert.public_key().public_numbers()
        key_public_numbers = key.public_key().public_numbers()
        
        if cert_public_numbers.n != key_public_numbers.n or cert_public_numbers.e != key_public_numbers.e:
            raise ValueError("Certificate and private key do not match")
        
        return True
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Failed to validate certificate/key match: {exc}") from exc


def _get_certificate_expiration(cert_path: str) -> datetime.datetime | None:
    """Extract the expiration date from a certificate file.
    
    Args:
        cert_path: Path to the certificate file (PEM format)
    
    Returns:
        Expiration datetime in UTC, or None if unable to read
    """
    try:
        with open(cert_path, "rb") as f:
            cert_data = f.read()
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        return cert.not_valid_after_utc
    except Exception as exc:
        logger.warning("Failed to read certificate expiration from %s: %s", cert_path, exc)
        return None


def _is_self_signed_cert(cert_path: str) -> bool:
    """Check if a certificate is self-signed.
    
    Args:
        cert_path: Path to the certificate file (PEM format)
    
    Returns:
        True if self-signed, False otherwise
    """
    try:
        with open(cert_path, "rb") as f:
            cert_data = f.read()
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        # A certificate is self-signed if issuer == subject
        return cert.issuer == cert.subject
    except Exception as exc:
        logger.warning("Failed to check if certificate is self-signed: %s", exc)
        return False


def get_or_create_self_signed_cert(cert_dir: str = "/data/certs", hostname: str = "localhost", key_size: int = 4096, encrypt_key: bool = False) -> tuple[str, str]:
    """Get or create a self-signed certificate for development/testing.
    
    Regenerates the certificate if it expires within CERT_REGENERATION_THRESHOLD_DAYS.
    
    Args:
        cert_dir: Directory to store certificates (default: /data/certs)
        hostname: Hostname for the certificate CN (default: localhost)
        key_size: RSA key size in bits (default: 4096)
        encrypt_key: Whether to encrypt the private key with a passphrase (default: False)
    
    Returns:
        Tuple of (cert_path, key_path)
    
    Raises:
        RuntimeError: If certificate generation fails
    """
    cert_dir_path = Path(cert_dir)
    cert_path = cert_dir_path / "server.crt"
    key_path = cert_dir_path / "server.key"
    
    if cert_path.exists() and key_path.exists():
        expiration = _get_certificate_expiration(str(cert_path))
        if expiration:
            time_delta = expiration - datetime.datetime.now(datetime.timezone.utc)
            days_until_expiry = time_delta.total_seconds() / 86400
            logger.info("Self-signed certificate expires in %.2f days", days_until_expiry)
            
            if days_until_expiry > CERT_REGENERATION_THRESHOLD_DAYS:
                logger.info("Using existing self-signed certificate at %s", cert_path)
                return str(cert_path), str(key_path)
            
            logger.warning("Self-signed certificate expires in %.2f days; regenerating...", days_until_expiry)
        else:
            logger.info("Using existing self-signed certificate at %s", cert_path)
            return str(cert_path), str(key_path)
    
    try:
        cert_dir_path.mkdir(parents=True, exist_ok=True)
        os.chmod(str(cert_dir_path), 0o700)
    except Exception as exc:
        raise RuntimeError(f"Failed to create certificate directory {cert_dir}: {exc}") from exc
    
    logger.info("Generating self-signed certificate for %s", hostname)
    
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
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
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            x509.IPAddress(ipaddress.IPv6Address("::1")),  # IPv6 loopback
        ]),
        critical=False,
    ).sign(private_key, hashes.SHA256(), default_backend())
    
    if encrypt_key:
        encryption = serialization.BestAvailableEncryption(b"development-key-passphrase")
        logger.warning("⚠️  Private key will be encrypted with default development passphrase")
    else:
        encryption = serialization.NoEncryption()
    
    with open(key_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=encryption
        ))
    
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    os.chmod(key_path, 0o600)
    os.chmod(cert_path, 0o644)
    
    logger.info("Self-signed certificate generated at %s (valid for 365 days, %d-bit RSA)", cert_path, key_size)
    logger.warning("⚠️  Using self-signed certificate. This is only suitable for development/testing.")
    logger.warning("⚠️  For production, provide a proper certificate via SSL_CERT_FILE and SSL_KEY_FILE.")
    
    return str(cert_path), str(key_path)



def validate_certificate_files(cert_path: str | None, key_path: str | None) -> tuple[str, str]:
    """Validate that certificate and key files exist and are readable.
    
    If neither is provided, generates self-signed certificates.
    If only one is provided, raises an error.
    
    For user-provided certificates: validates that they exist, are readable, and NOT expired.
    For self-signed certificates: validates and regenerates if expiring within threshold.
    
    Args:
        cert_path: Path to certificate file (or None)
        key_path: Path to key file (or None)
    
    Returns:
        Tuple of (cert_path, key_path)
    
    Raises:
        ValueError: If configuration is invalid or user cert is expired
        RuntimeError: If certificate generation fails
    """
    cert_exists = cert_path and os.path.isfile(cert_path)
    key_exists = key_path and os.path.isfile(key_path)
    
    if cert_exists and key_exists:
        logger.debug("Validating user-provided certificate: %s", cert_path)
        try:
            # Validate PEM format for both certificate and key
            _validate_pem_format(cert_path, "certificate")
            _validate_pem_format(key_path, "key")
            # Validate that certificate and key match
            _validate_cert_key_match(cert_path, key_path)
        except ValueError as exc:
            raise ValueError(f"Certificate validation failed: {exc}") from exc
        except Exception as exc:
            raise ValueError(f"Cannot read certificate files: {exc}") from exc
        
        # Check if user-provided certificate is expired
        is_self_signed = _is_self_signed_cert(cert_path)
        expiration = _get_certificate_expiration(cert_path)
        
        if expiration:
            now = datetime.datetime.now(datetime.timezone.utc)
            time_delta = expiration - now
            days_until_expiry = time_delta.total_seconds() / 86400
            
            if days_until_expiry <= 0:
                raise ValueError(
                    f"User-provided SSL certificate has EXPIRED (expiration: {expiration.isoformat()}). "
                    f"Please provide a valid certificate via SSL_CERT_FILE and SSL_KEY_FILE."
                )
            
            if days_until_expiry < 30:
                logger.warning(
                    "⚠️  User-provided SSL certificate expires in %d days (%s). "
                    "Please plan to renew your certificate.",
                    days_until_expiry, expiration.isoformat()
                )
            else:
                logger.info("User-provided certificate is valid (expires in %d days)", days_until_expiry)
        
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
