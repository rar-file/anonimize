"""Credit card anonymizer."""

import re
import hashlib
from typing import Optional


class CreditCardAnonymizer:
    """Anonymizer for credit card numbers.
    
    Supports strategies:
    - replace: Replace with valid-looking fake
    - hash: One-way hash
    - mask: Show only last 4 (XXXX-XXXX-XXXX-1234)
    - last4: Return just last 4
    - token: Generate reversible token (if key provided)
    """
    
    # Patterns for major card types
    CARD_PATTERNS = {
        'visa': re.compile(r'\b4\d{3}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'),
        'mastercard': re.compile(r'\b5[1-5]\d{2}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'),
        'amex': re.compile(r'\b3[47]\d{2}[-\s]?\d{6}[-\s]?\d{5}\b'),
        'discover': re.compile(r'\b6(?:011|5\d{2})[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'),
    }
    
    def __init__(self, phoney=None, token_key: Optional[str] = None):
        self._phoney = phoney
        self._token_key = token_key
        self._cache = {}
    
    def anonymize(self, card: str, strategy: str = "mask") -> str:
        """Anonymize a credit card number."""
        normalized = self._normalize(card)
        if not normalized or not self._luhn_check(normalized):
            return card
        
        if strategy == "replace":
            return self._replace(card, normalized)
        elif strategy == "hash":
            return self._hash(card)
        elif strategy == "mask":
            return self._mask(normalized)
        elif strategy == "last4":
            return self._last4(normalized)
        elif strategy == "token":
            return self._token(card, normalized)
        else:
            return self._mask(normalized)
    
    def _normalize(self, card: str) -> str:
        """Extract digits only."""
        return re.sub(r'\D', '', card)
    
    def _luhn_check(self, card_number: str) -> bool:
        """Validate card with Luhn algorithm."""
        if not card_number.isdigit():
            return False
        
        digits = [int(d) for d in card_number]
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        
        checksum = sum(odd_digits)
        for d in even_digits:
            checksum += sum(divmod(d * 2, 10))
        
        return checksum % 10 == 0
    
    def _replace(self, original: str, normalized: str) -> str:
        """Replace with valid fake card."""
        if original in self._cache:
            return self._cache[original]
        
        # Generate fake card starting with same prefix
        import random
        random.seed(int(hashlib.md5(normalized.encode()).hexdigest(), 16))
        
        prefix = normalized[0]
        if prefix == '4':  # Visa
            fake = '4' + ''.join([str(random.randint(0, 9)) for _ in range(15)])
        elif prefix == '5':  # Mastercard
            fake = '5' + str(random.randint(1, 5)) + ''.join([str(random.randint(0, 9)) for _ in range(14)])
        else:
            fake = '4' + ''.join([str(random.randint(0, 9)) for _ in range(15)])
        
        # Fix checksum
        fake = self._fix_luhn(fake)
        formatted = f"{fake[:4]}-{fake[4:8]}-{fake[8:12]}-{fake[12:]}"
        
        self._cache[original] = formatted
        return formatted
    
    def _fix_luhn(self, card_number: str) -> str:
        """Adjust last digit to make Luhn valid."""
        digits = [int(d) for d in card_number[:-1]]
        checksum = 0
        
        # Reverse for Luhn
        for i, d in enumerate(reversed(digits)):
            if i % 2 == 0:
                d *= 2
                if d > 9:
                    d -= 9
            checksum += d
        
        check_digit = (10 - (checksum % 10)) % 10
        return ''.join(map(str, digits)) + str(check_digit)
    
    def _hash(self, card: str) -> str:
        """One-way hash."""
        return "CARD-" + hashlib.sha256(card.encode()).hexdigest()[:16].upper()
    
    def _mask(self, normalized: str) -> str:
        """Show only last 4."""
        if len(normalized) == 15:  # Amex
            return f"XXXX-XXXXXX-X{normalized[11:]}"
        return f"XXXX-XXXX-XXXX-{normalized[12:]}"
    
    def _last4(self, normalized: str) -> str:
        """Return last 4 only."""
        return normalized[-4:]
    
    def _token(self, original: str, normalized: str) -> str:
        """Generate reversible token if key provided."""
        if not self._token_key:
            return self._mask(normalized)
        
        from cryptography.fernet import Fernet
        f = Fernet(self._token_key.encode())
        token = f.encrypt(normalized.encode()).decode()
        return f"TKN-{token[:20]}..."
    
    def detect(self, text: str) -> list:
        """Detect all credit cards in text."""
        matches = []
        for pattern in self.CARD_PATTERNS.values():
            matches.extend(pattern.findall(text))
        return list(set(matches))
    
    def get_card_type(self, card: str) -> Optional[str]:
        """Detect card type from number."""
        normalized = self._normalize(card)
        if not normalized:
            return None
        
        if normalized.startswith('4'):
            return 'visa'
        elif normalized.startswith('5') and normalized[1] in '12345':
            return 'mastercard'
        elif normalized.startswith('34') or normalized.startswith('37'):
            return 'amex'
        elif normalized.startswith('6011') or normalized.startswith('65'):
            return 'discover'
        return 'unknown'
