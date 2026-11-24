"""Ephemeral local trust: a throwaway CA and a loopback server certificate.

Nothing here is ever committed. Keys are generated in memory at fixture start,
written to a private state directory only because ``ssl.SSLContext.load_cert_chain``
needs files, and removed again when the fixture stops.
"""

from __future__ import annotations

import datetime as _dt
import ipaddress
import os
import secrets
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


@dataclass(frozen=True)
class TrustFiles:
    ca_file: Path  # PEM CA certificate: the only file a client needs
    cert_file: Path  # PEM server leaf certificate
    key_file: Path  # PEM server private key (0600)
    ca_fingerprint_sha256: str

    def paths(self) -> list[Path]:
        return [self.ca_file, self.cert_file, self.key_file]


def _new_key(key_type: str):
    if key_type == "ec":
        return ec.generate_private_key(ec.SECP256R1())
    if key_type == "rsa":
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)
    raise ValueError(f"unknown key_type {key_type!r} (expected 'ec' or 'rsa')")


def generate(state_dir: Path, key_type: str = "ec", hosts: tuple[str, ...] = ("127.0.0.1", "::1", "localhost")) -> TrustFiles:
    """Create a fresh CA + server leaf valid for the given loopback names/IPs."""
    state_dir.mkdir(parents=True, exist_ok=True)
    now = _dt.datetime.now(_dt.timezone.utc)
    not_before = now - _dt.timedelta(minutes=5)
    not_after = now + _dt.timedelta(days=1)
    tag = secrets.token_hex(4)

    ca_key = _new_key(key_type)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"telelem-wire ephemeral CA {tag}")])
    ca_ski = x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key())
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False, key_cert_sign=True,
                crl_sign=True, encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(ca_ski, critical=False)
        .sign(ca_key, hashes.SHA256())
    )

    leaf_key = _new_key(key_type)
    sans: list[x509.GeneralName] = []
    for h in hosts:
        try:
            sans.append(x509.IPAddress(ipaddress.ip_address(h)))
        except ValueError:
            sans.append(x509.DNSName(h))
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "telelem-wire loopback")]))
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=(key_type == "rsa"), data_encipherment=False,
                key_agreement=False, key_cert_sign=False, crl_sign=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(leaf_key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ca_ski), critical=False)
        .sign(ca_key, hashes.SHA256())
    )

    ca_file = state_dir / "ca.pem"
    cert_file = state_dir / "server.pem"
    key_file = state_dir / "server.key"
    ca_file.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_file.write_bytes(leaf_cert.public_bytes(serialization.Encoding.PEM))
    fd = os.open(key_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(
            leaf_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
    fp = ca_cert.fingerprint(hashes.SHA256()).hex()
    return TrustFiles(ca_file=ca_file, cert_file=cert_file, key_file=key_file, ca_fingerprint_sha256=fp)
