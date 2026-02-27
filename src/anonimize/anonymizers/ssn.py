"""SSN (Social Security Number) anonymizer."""

import re
import hashlib


class SSNAnonymizer:
    """Anonymizer for US Social Security Numbers.
    
    Supports strategies:
    - replace: Replace with fake SSN
    - hash: One-way hash
    - mask: Standard masking (XXX-XX-1234)
    - last4: Keep only last 4
    - invalid: Replace with invalid SSN (000-XX-XXXX)
    """
    
    SSN_PATTERN = re.compile(
        r'\b(?!000|666|9\d{2})\d{3}-?(?!00)\d{2}-?(?!0000)\d{4}\b'
    )
    
    def __init__(self, phoney=None):
        self._phoney = phoney
        self._cache = {}
    
    def anonymize(self, ssn: str, strategy: str = "mask") -> str:
        """Anonymize an SSN."""
        normalized = self._normalize(ssn)
        if not normalized:
            return ssn
        
        if strategy == "replace":
            return self._replace(ssn, normalized)
        elif strategy == "hash":
            return self._hash(ssn)
        elif strategy == "mask":
            return self._mask(normalized)
        elif strategy == "last4":
            return self._last4(normalized)
        elif strategy == "invalid":
            return self._invalid(ssn, normalized)
        else:
            return self._mask(normalized)
    
    def _normalize(self, ssn: str) -> str:
        """Extract digits only."""
        digits = re.sub(r'\D', '', ssn)
        return digits if len(digits) == 9 else ""
    
    def _replace(self, original: str, normalized: str) -> str:
        """Replace with valid fake SSN."""
        if original in self._cache:
            return self._cache[original]
        
        # Generate valid SSN (avoid 000, 666, 900-999 prefixes)
        import random
        random.seed(int(hashlib.md5(normalized.encode()).hexdigest(), 16))
        
        area = random.choice([str(i).zfill(3) for i in range(1, 900) if i not in [0, 666]])
        group = str(random.randint(1, 99)).zfill(2)
        serial = str(random.randint(1, 9999)).zfill(4)
        
        fake = f"{area}-{group}-{serial}"
        self._cache[original] = fake
        return fake
    
    def _hash(self, ssn: str) -> str:
        """One-way hash."""
        return "SSN-" + hashlib.sha256(ssn.encode()).hexdigest()[:12].upper()
    
    def _mask(self, normalized: str) -> str:
        """Standard mask: XXX-XX-1234"""
        return f"XXX-XX-{normalized[5:]}"
    
    def _last4(self, normalized: str) -> str:
        """Last 4 only: ****-****-1234"""
        return f"****-****-{normalized[5:]}"
    
    def _invalid(self, original: str, normalized: str) -> str:
        """Replace with invalid SSN pattern."""
        if original in self._cache:
            return self._cache[original]
        
        # Use 000 prefix (invalid)
        fake = f"000-{normalized[3:5]}-{normalized[5:]}"
        self._cache[original] = fake
        return fake
    
    def detect(self, text: str) -> list:
        """Detect all SSNs in text."""
        return self.SSN_PATTERN.findall(text)
    
    def is_valid(self, ssn: str) -> bool:
        """Check if SSN format is valid."""
        normalized = self._normalize(ssn)
        if len(normalized) != 9:
            return False
        
        area = int(normalized[:3])
        group = int(normalized[3:5])
        serial = int(normalized[5:])
        
        # Check invalid ranges
        if area in [0, 666] or area >= 900:
            return False
        if group == 0:
            return False
        if serial == 0:
            return False
        
        return True
