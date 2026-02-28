"""Differential Privacy mechanisms for anonimize.

This module implements various differential privacy mechanisms including
Laplace, Gaussian, and Exponential mechanisms, along with privacy budget
tracking and sensitivity calculations.

References:
    - Dwork, C. & Roth, A. (2014). The Algorithmic Foundations of Differential Privacy.
    - Google DP Library: https://github.com/google/differential-privacy
"""

import logging
import math
import random
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class NoiseMechanism(Enum):
    """Supported noise mechanisms."""

    LAPLACE = "laplace"
    GAUSSIAN = "gaussian"


@dataclass(frozen=True)
class PrivacyParameters:
    """Parameters for differential privacy.

    Attributes:
        epsilon: Privacy parameter (smaller = more private, typically 0.1-10)
        delta: Approximation parameter for (ε,δ)-DP (typically 0 for pure DP)
        sensitivity: Maximum change in query output from adding/removing one record
    """

    epsilon: float
    delta: float = 0.0
    sensitivity: float = 1.0

    def __post_init__(self):
        if self.epsilon <= 0:
            raise ValueError(f"epsilon must be positive, got {self.epsilon}")
        if self.delta < 0 or self.delta >= 1:
            raise ValueError(f"delta must be in [0, 1), got {self.delta}")
        if self.sensitivity <= 0:
            raise ValueError(f"sensitivity must be positive, got {self.sensitivity}")


class DifferentialPrivacyError(Exception):
    """Exception raised for differential privacy errors."""

    pass


class PrivacyBudgetExceeded(DifferentialPrivacyError):
    """Exception raised when privacy budget is exceeded."""

    pass


class Mechanism(ABC):
    """Abstract base class for differential privacy mechanisms."""

    def __init__(self, params: PrivacyParameters):
        """Initialize the mechanism.

        Args:
            params: Privacy parameters (epsilon, delta, sensitivity)
        """
        self.params = params
        self._rng = random.Random()

    def seed(self, seed_value: int) -> None:
        """Set random seed for reproducibility.

        Args:
            seed_value: Random seed
        """
        self._rng = random.Random(seed_value)

    @abstractmethod
    def add_noise(self, value: float) -> float:
        """Add noise to a value.

        Args:
            value: The true value to add noise to

        Returns:
            Noisy value
        """
        pass

    @abstractmethod
    def get_noise_scale(self) -> float:
        """Get the noise scale parameter.

        Returns:
            Noise scale (b for Laplace, sigma for Gaussian)
        """
        pass


class LaplaceMechanism(Mechanism):
    """Laplace mechanism for ε-differential privacy.

    Adds Laplace-distributed noise calibrated to the L1 sensitivity.
    Provides pure ε-differential privacy (δ = 0).

    The noise scale b = sensitivity / epsilon
    """

    def add_noise(self, value: float) -> float:
        """Add Laplace noise to a value.

        Args:
            value: True value

        Returns:
            Value with Laplace noise added
        """
        scale = self.get_noise_scale()
        # Generate Laplace noise using inverse transform sampling
        u = self._rng.random() - 0.5
        noise = -scale * math.copysign(1.0, u) * math.log(1 - 2 * abs(u))
        return value + noise

    def get_noise_scale(self) -> float:
        """Get Laplace noise scale b = sensitivity / epsilon.

        Returns:
            Noise scale parameter
        """
        return self.params.sensitivity / self.params.epsilon

    def confidence_interval(
        self, value: float, confidence: float = 0.95
    ) -> Tuple[float, float]:
        """Compute confidence interval for the noisy value.

        Args:
            value: Noisy observed value
            confidence: Confidence level (default 0.95)

        Returns:
            (lower_bound, upper_bound) tuple
        """
        if not 0 < confidence < 1:
            raise ValueError(f"confidence must be in (0, 1), got {confidence}")

        scale = self.get_noise_scale()
        alpha = 1 - confidence
        # For Laplace, the CDF is F(x) = 0.5 * exp(x/b) for x < 0
        # and F(x) = 1 - 0.5 * exp(-x/b) for x >= 0
        # Quantile at p is: -b * sign(p-0.5) * ln(1 - 2|p-0.5|)
        quantile = -scale * math.log(alpha)
        return (value - quantile, value + quantile)


class GaussianMechanism(Mechanism):
    """Gaussian mechanism for (ε,δ)-differential privacy.

    Adds Gaussian-distributed noise calibrated to the L2 sensitivity.
    Provides (ε,δ)-differential privacy where δ > 0.

    The noise scale sigma = sensitivity * sqrt(2 * ln(1.25/δ)) / epsilon
    """

    def add_noise(self, value: float) -> float:
        """Add Gaussian noise to a value.

        Args:
            value: True value

        Returns:
            Value with Gaussian noise added
        """
        scale = self.get_noise_scale()
        # Box-Muller transform for Gaussian random numbers
        u1 = self._rng.random()
        u2 = self._rng.random()
        z = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
        return value + scale * z

    def get_noise_scale(self) -> float:
        """Get Gaussian noise scale sigma.

        Returns:
            Standard deviation of noise
        """
        if self.params.delta == 0:
            raise DifferentialPrivacyError(
                "Gaussian mechanism requires delta > 0 for (ε,δ)-DP"
            )

        # Calibrate noise for (ε,δ)-DP
        # sigma >= sensitivity * sqrt(2 * ln(1.25/δ)) / ε
        calibration = math.sqrt(2 * math.log(1.25 / self.params.delta))
        return self.params.sensitivity * calibration / self.params.epsilon

    def confidence_interval(
        self, value: float, confidence: float = 0.95
    ) -> Tuple[float, float]:
        """Compute confidence interval for the noisy value.

        Args:
            value: Noisy observed value
            confidence: Confidence level (default 0.95)

        Returns:
            (lower_bound, upper_bound) tuple
        """
        if not 0 < confidence < 1:
            raise ValueError(f"confidence must be in (0, 1), got {confidence}")

        scale = self.get_noise_scale()
        # For 95% confidence, z ≈ 1.96
        # For general confidence, use inverse error function
        alpha = 1 - confidence
        z = math.sqrt(2) * self._inverse_erf(1 - alpha)
        margin = z * scale
        return (value - margin, value + margin)

    def _inverse_erf(self, x: float) -> float:
        """Approximate inverse error function using Abramowitz & Stegun formula.

        Args:
            x: Input in (-1, 1)

        Returns:
            Approximation of erf^-1(x)
        """
        # Abramowitz & Stegun formula 7.1.26
        a = 8 * (math.pi - 3) / (3 * math.pi * (4 - math.pi))
        y = math.log(1 - x * x)
        z = 2 / (math.pi * a) + y / 2
        return math.copysign(1.0, x) * math.sqrt(math.sqrt(z * z - y / a) - z)


class PrivacyBudgetTracker:
    """Track and manage privacy budget across multiple queries.

    Implements privacy budget accounting using basic composition or
    advanced composition theorems.

    Attributes:
        total_epsilon: Total privacy budget (epsilon)
        total_delta: Total privacy budget (delta)
        used_epsilon: Currently consumed epsilon
        used_delta: Currently consumed delta
    """

    def __init__(
        self, total_epsilon: float, total_delta: float = 0.0, composition: str = "basic"
    ):
        """Initialize the privacy budget tracker.

        Args:
            total_epsilon: Total epsilon budget
            total_delta: Total delta budget
            composition: Composition theorem ('basic' or 'advanced')
        """
        if total_epsilon <= 0:
            raise ValueError(f"total_epsilon must be positive, got {total_epsilon}")
        if total_delta < 0 or total_delta >= 1:
            raise ValueError(f"total_delta must be in [0, 1), got {total_delta}")

        self.total_epsilon = total_epsilon
        self.total_delta = total_delta
        self.used_epsilon = 0.0
        self.used_delta = 0.0
        self.composition = composition
        self._query_history: List[Dict[str, Any]] = []

        logger.debug(
            f"PrivacyBudgetTracker initialized: ε={total_epsilon}, δ={total_delta}"
        )

    def consume(self, epsilon: float, delta: float = 0.0) -> None:
        """Consume privacy budget.

        Args:
            epsilon: Epsilon to consume
            delta: Delta to consume

        Raises:
            PrivacyBudgetExceeded: If budget would be exceeded
        """
        if epsilon < 0 or delta < 0:
            raise ValueError("Cannot consume negative budget")

        new_epsilon = self.used_epsilon + epsilon
        new_delta = self.used_delta + delta

        if new_epsilon > self.total_epsilon or new_delta > self.total_delta:
            raise PrivacyBudgetExceeded(
                f"Privacy budget exceeded: would use ε={new_epsilon:.4f} "
                f"(limit: {self.total_epsilon:.4f}), δ={new_delta:.6f} "
                f"(limit: {self.total_delta:.6f})"
            )

        self.used_epsilon = new_epsilon
        self.used_delta = new_delta
        self._query_history.append({"epsilon": epsilon, "delta": delta})

        logger.debug(f"Consumed privacy budget: ε={epsilon:.4f}, δ={delta:.6f}")

    def remaining(self) -> Tuple[float, float]:
        """Get remaining privacy budget.

        Returns:
            (remaining_epsilon, remaining_delta) tuple
        """
        return (
            self.total_epsilon - self.used_epsilon,
            self.total_delta - self.used_delta,
        )

    def get_usage_report(self) -> Dict[str, Any]:
        """Get detailed privacy budget usage report.

        Returns:
            Dictionary with usage statistics
        """
        remaining_eps, remaining_delta = self.remaining()
        return {
            "total_epsilon": self.total_epsilon,
            "total_delta": self.total_delta,
            "used_epsilon": self.used_epsilon,
            "used_delta": self.used_delta,
            "remaining_epsilon": remaining_eps,
            "remaining_delta": remaining_delta,
            "epsilon_percentage_used": (self.used_epsilon / self.total_epsilon) * 100,
            "delta_percentage_used": (
                (self.used_delta / self.total_delta) * 100
                if self.total_delta > 0
                else 0
            ),
            "query_count": len(self._query_history),
        }

    def reset(self) -> None:
        """Reset the privacy budget (use with caution - breaks privacy guarantees)."""
        logger.warning(
            "Privacy budget reset - this breaks differential privacy guarantees"
        )
        self.used_epsilon = 0.0
        self.used_delta = 0.0
        self._query_history.clear()

    @contextmanager
    def query_context(self, epsilon: float, delta: float = 0.0):
        """Context manager for consuming budget on query execution.

        Args:
            epsilon: Epsilon cost of the query
            delta: Delta cost of the query

        Yields:
            Self for potential nested tracking

        Example:
            >>> tracker = PrivacyBudgetTracker(total_epsilon=1.0)
            >>> with tracker.query_context(epsilon=0.1):
            ...     result = mechanism.add_noise(value)
        """
        self.consume(epsilon, delta)
        try:
            yield self
        except Exception:
            # In a real implementation, you might want to refund the budget
            # or handle this differently depending on whether the query completed
            raise


class SensitivityCalculator:
    """Calculate sensitivity of queries and data transformations.

    Sensitivity is the maximum change in the output of a function
    when one record is added to or removed from the dataset.
    """

    @staticmethod
    def l1_sensitivity(values: List[float], add_one: float) -> float:
        """Calculate L1 sensitivity by adding a record.

        Args:
            values: Current dataset values
            add_one: Value of the record to add

        Returns:
            L1 sensitivity
        """
        # For sum: sensitivity is max possible value
        # For count: sensitivity is 1
        # This is a simplified version - real implementation needs query type
        return abs(add_one)

    @staticmethod
    def l2_sensitivity(values: List[float], add_one: float) -> float:
        """Calculate L2 sensitivity by adding a record.

        Args:
            values: Current dataset values
            add_one: Value of the record to add

        Returns:
            L2 sensitivity
        """
        return abs(add_one)  # Simplified

    @staticmethod
    def bounded_sensitivity(
        lower_bound: float, upper_bound: float, query_type: str = "sum"
    ) -> float:
        """Calculate sensitivity for bounded data.

        Args:
            lower_bound: Minimum possible value
            upper_bound: Maximum possible value
            query_type: Type of query ('sum', 'count', 'mean', etc.)

        Returns:
            Sensitivity value
        """
        if query_type == "sum":
            return max(abs(lower_bound), abs(upper_bound))
        elif query_type == "count":
            return 1.0
        elif query_type == "mean":
            # Mean sensitivity depends on dataset size
            # This is an upper bound approximation
            return upper_bound - lower_bound  # Simplified
        else:
            raise ValueError(f"Unknown query type: {query_type}")


class DPAnonymizer:
    """Main differential privacy anonymizer with budget tracking.

    This class provides a high-level interface for applying differential
    privacy to data anonymization tasks.

    Example:
        >>> dp = DPAnonymizer(total_epsilon=1.0, mechanism="laplace")
        >>> noisy_value = dp.anonymize_numeric(100.0, sensitivity=1.0)
    """

    def __init__(
        self,
        total_epsilon: float,
        total_delta: float = 0.0,
        mechanism: str = "laplace",
        seed: Optional[int] = None,
    ):
        """Initialize the DP anonymizer.

        Args:
            total_epsilon: Total privacy budget for epsilon
            total_delta: Total privacy budget for delta
            mechanism: Noise mechanism ('laplace' or 'gaussian')
            seed: Random seed for reproducibility
        """
        self.budget = PrivacyBudgetTracker(total_epsilon, total_delta)
        self.mechanism_type = mechanism
        self.seed = seed

        logger.info(
            f"DPAnonymizer initialized with {mechanism} mechanism, "
            f"ε={total_epsilon}, δ={total_delta}"
        )

    def anonymize_numeric(
        self, value: float, sensitivity: float, epsilon: Optional[float] = None
    ) -> float:
        """Anonymize a numeric value with differential privacy.

        Args:
            value: True value to anonymize
            sensitivity: L1/L2 sensitivity of the query
            epsilon: Privacy parameter (uses budget allocation if None)

        Returns:
            Noisy value
        """
        eps = epsilon or (self.budget.total_epsilon / 10)  # Conservative default
        delta = (
            0.0 if self.mechanism_type == "laplace" else self.budget.total_delta / 10
        )

        params = PrivacyParameters(epsilon=eps, delta=delta, sensitivity=sensitivity)

        if self.mechanism_type == "laplace":
            mechanism = LaplaceMechanism(params)
        elif self.mechanism_type == "gaussian":
            mechanism = GaussianMechanism(params)
        else:
            raise ValueError(f"Unknown mechanism: {self.mechanism_type}")

        if self.seed is not None:
            mechanism.seed(self.seed)

        with self.budget.query_context(eps, delta):
            return mechanism.add_noise(value)

    def anonymize_count(self, count: int, epsilon: Optional[float] = None) -> float:
        """Anonymize a count with differential privacy.

        Args:
            count: True count value
            epsilon: Privacy parameter

        Returns:
            Noisy count
        """
        return self.anonymize_numeric(float(count), sensitivity=1.0, epsilon=epsilon)

    def anonymize_sum(
        self,
        sum_value: float,
        lower_bound: float,
        upper_bound: float,
        epsilon: Optional[float] = None,
    ) -> float:
        """Anonymize a sum with differential privacy.

        Args:
            sum_value: True sum value
            lower_bound: Lower bound of data (for clipping)
            upper_bound: Upper bound of data (for clipping)
            epsilon: Privacy parameter

        Returns:
            Noisy sum
        """
        sensitivity = max(abs(lower_bound), abs(upper_bound))
        return self.anonymize_numeric(
            sum_value, sensitivity=sensitivity, epsilon=epsilon
        )

    def get_budget_report(self) -> Dict[str, Any]:
        """Get privacy budget usage report.

        Returns:
            Budget usage dictionary
        """
        return self.budget.get_usage_report()
