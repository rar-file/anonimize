"""Email anonymizer for email addresses."""

import hashlib
import re


class EmailAnonymizer:
    """Anonymizer for email addresses.

    Supports strategies:
    - replace: Replace with fake email
    - hash: One-way hash of email
    - mask: Partial masking (s***@example.com)
    - domain_only: Keep domain only (***@example.com)
    """

    EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")

    def __init__(self, phoney=None):
        self._phoney = phoney
        self._cache = {}

    def anonymize(self, email: str, strategy: str = "replace") -> str:
        """Anonymize an email address.

        Args:
            email: The email to anonymize
            strategy: One of replace, hash, mask, domain_only

        Returns:
            Anonymized email
        """
        if not email or "@" not in email:
            return email

        if strategy == "replace":
            return self._replace(email)
        elif strategy == "hash":
            return self._hash(email)
        elif strategy == "mask":
            return self._mask(email)
        elif strategy == "domain_only":
            return self._domain_only(email)
        else:
            return self._replace(email)

    def _replace(self, email: str) -> str:
        """Replace with consistent fake email."""
        if email in self._cache:
            return self._cache[email]

        if self._phoney:
            fake = self._phoney.email()
        else:
            # Generate deterministic fake
            local, domain = email.split("@")
            fake_local = f"user{hash(local) % 10000}"
            fake = f"{fake_local}@example.com"

        self._cache[email] = fake
        return fake

    def _hash(self, email: str) -> str:
        """One-way hash of email."""
        return hashlib.sha256(email.encode()).hexdigest()[:16] + "@hashed"

    def _mask(self, email: str) -> str:
        """Mask local part: j***@example.com"""
        local, domain = email.split("@")
        if len(local) <= 2:
            return f"**@{domain}"
        return f"{local[0]}{'*' * (len(local)-1)}@{domain}"

    def _domain_only(self, email: str) -> str:
        """Keep only domain: ***@example.com"""
        _, domain = email.split("@")
        return f"***@{domain}"

    def detect(self, text: str) -> list:
        """Detect all emails in text."""
        return self.EMAIL_PATTERN.findall(text)
