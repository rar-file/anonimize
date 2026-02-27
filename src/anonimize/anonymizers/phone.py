"""Phone number anonymizer."""

import re
import hashlib
from typing import Optional


class PhoneAnonymizer:
    """Anonymizer for phone numbers.
    
    Supports strategies:
    - replace: Replace with fake phone
    - hash: One-way hash
    - mask: Partial masking
    - last4: Keep only last 4 digits
    """
    
    # US phone patterns
    PHONE_PATTERNS = [
        re.compile(r'\b\d{3}-\d{3}-\d{4}\b'),  # 555-555-5555
        re.compile(r'\(\d{3}\)\s*\d{3}-\d{4}'),  # (555) 555-5555
        re.compile(r'\b\d{3}\.\d{3}\.\d{4}\b'),  # 555.555.5555
        re.compile(r'\b\d{10}\b'),  # 5555555555
        re.compile(r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),  # +1 formats
    ]
    
    def __init__(self, phoney=None):
        self._phoney = phoney
        self._cache = {}
    
    def anonymize(self, phone: str, strategy: str = "replace") -> str:
        """Anonymize a phone number."""
        normalized = self._normalize(phone)
        if not normalized:
            return phone
        
        if strategy == "replace":
            return self._replace(phone, normalized)
        elif strategy == "hash":
            return self._hash(phone)
        elif strategy == "mask":
            return self._mask(normalized)
        elif strategy == "last4":
            return self._last4(normalized)
        else:
            return self._replace(phone, normalized)
    
    def _normalize(self, phone: str) -> str:
        """Extract digits only."""
        digits = re.sub(r'\D', '', phone)
        # Handle US numbers with country code
        if len(digits) == 11 and digits.startswith('1'):
            digits = digits[1:]
        return digits if len(digits) == 10 else ""
    
    def _replace(self, original: str, normalized: str) -> str:
        """Replace with consistent fake phone."""
        if original in self._cache:
            return self._cache[original]
        
        if self._phoney and hasattr(self._phoney, 'phone'):
            fake = self._phoney.phone()
        else:
            # Generate fake: (555) XXX-XXXX where XXX is hash-based
            fake_prefix = str(int(hashlib.md5(normalized.encode()).hexdigest(), 16) % 900 + 100)
            fake_suffix = str(int(hashlib.md5((normalized + "salt").encode()).hexdigest(), 16) % 10000).zfill(4)
            fake = f"(555) {fake_prefix}-{fake_suffix}"
        
        self._cache[original] = fake
        return fake
    
    def _hash(self, phone: str) -> str:
        """One-way hash."""
        return hashlib.sha256(phone.encode()).hexdigest()[:10]
    
    def _mask(self, normalized: str) -> str:
        """Mask: (***) ***-1234"""
        return f"(***) ***-{normalized[6:]}"
    
    def _last4(self, normalized: str) -> str:
        """Keep last 4: ****-****-1234"""
        return f"****-****-{normalized[6:]}"
    
    def detect(self, text: str) -> list:
        """Detect all phone numbers in text."""
        matches = []
        for pattern in self.PHONE_PATTERNS:
            matches.extend(pattern.findall(text))
        return list(set(matches))
