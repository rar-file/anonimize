"""Tests for differential privacy module."""

import pytest
import math
from anonimize.differential_privacy import (
    PrivacyParameters,
    LaplaceMechanism,
    GaussianMechanism,
    PrivacyBudgetTracker,
    SensitivityCalculator,
    DPAnonymizer,
    DifferentialPrivacyError,
    PrivacyBudgetExceeded,
)


class TestPrivacyParameters:
    """Test cases for PrivacyParameters dataclass."""

    def test_valid_parameters(self):
        """Test creation with valid parameters."""
        params = PrivacyParameters(epsilon=1.0, delta=0.0, sensitivity=1.0)
        assert params.epsilon == 1.0
        assert params.delta == 0.0
        assert params.sensitivity == 1.0

    def test_default_delta(self):
        """Test default delta is 0."""
        params = PrivacyParameters(epsilon=1.0)
        assert params.delta == 0.0

    def test_default_sensitivity(self):
        """Test default sensitivity is 1.0."""
        params = PrivacyParameters(epsilon=1.0)
        assert params.sensitivity == 1.0

    def test_invalid_epsilon_zero(self):
        """Test epsilon must be positive."""
        with pytest.raises(ValueError, match="epsilon must be positive"):
            PrivacyParameters(epsilon=0.0)

    def test_invalid_epsilon_negative(self):
        """Test epsilon cannot be negative."""
        with pytest.raises(ValueError, match="epsilon must be positive"):
            PrivacyParameters(epsilon=-1.0)

    def test_invalid_delta_negative(self):
        """Test delta cannot be negative."""
        with pytest.raises(ValueError, match="delta must be in \\[0, 1\"):
            PrivacyParameters(epsilon=1.0, delta=-0.1)

    def test_invalid_delta_one(self):
        """Test delta must be less than 1."""
        with pytest.raises(ValueError, match="delta must be in \\[0, 1\"):
            PrivacyParameters(epsilon=1.0, delta=1.0)

    def test_invalid_sensitivity(self):
        """Test sensitivity must be positive."""
        with pytest.raises(ValueError, match="sensitivity must be positive"):
            PrivacyParameters(epsilon=1.0, sensitivity=0.0)


class TestLaplaceMechanism:
    """Test cases for LaplaceMechanism."""

    def test_init(self):
        """Test mechanism initialization."""
        params = PrivacyParameters(epsilon=1.0, sensitivity=1.0)
        mechanism = LaplaceMechanism(params)
        assert mechanism.params == params

    def test_add_noise_changes_value(self):
        """Test that noise is actually added."""
        params = PrivacyParameters(epsilon=1.0, sensitivity=1.0)
        mechanism = LaplaceMechanism(params)
        
        true_value = 100.0
        noisy_value = mechanism.add_noise(true_value)
        
        assert noisy_value != true_value

    def test_noise_scale_calculation(self):
        """Test noise scale calculation."""
        params = PrivacyParameters(epsilon=2.0, sensitivity=1.0)
        mechanism = LaplaceMechanism(params)
        
        # b = sensitivity / epsilon = 1.0 / 2.0 = 0.5
        assert mechanism.get_noise_scale() == 0.5

    def test_noise_scale_with_different_sensitivity(self):
        """Test noise scale with different sensitivity."""
        params = PrivacyParameters(epsilon=1.0, sensitivity=5.0)
        mechanism = LaplaceMechanism(params)
        
        assert mechanism.get_noise_scale() == 5.0

    def test_seed_reproducibility(self):
        """Test that seed produces reproducible results."""
        params = PrivacyParameters(epsilon=1.0, sensitivity=1.0)
        
        mechanism1 = LaplaceMechanism(params)
        mechanism1.seed(42)
        
        mechanism2 = LaplaceMechanism(params)
        mechanism2.seed(42)
        
        result1 = mechanism1.add_noise(100.0)
        result2 = mechanism2.add_noise(100.0)
        
        assert result1 == result2

    def test_different_seeds_different_results(self):
        """Test that different seeds produce different results."""
        params = PrivacyParameters(epsilon=1.0, sensitivity=1.0)
        
        mechanism1 = LaplaceMechanism(params)
        mechanism1.seed(42)
        
        mechanism2 = LaplaceMechanism(params)
        mechanism2.seed(43)
        
        result1 = mechanism1.add_noise(100.0)
        result2 = mechanism2.add_noise(100.0)
        
        assert result1 != result2

    def test_confidence_interval(self):
        """Test confidence interval calculation."""
        params = PrivacyParameters(epsilon=1.0, sensitivity=1.0)
        mechanism = LaplaceMechanism(params)
        
        value = 100.0
        lower, upper = mechanism.confidence_interval(value, confidence=0.95)
        
        assert lower < value < upper

    def test_confidence_interval_invalid_confidence(self):
        """Test error on invalid confidence level."""
        params = PrivacyParameters(epsilon=1.0, sensitivity=1.0)
        mechanism = LaplaceMechanism(params)
        
        with pytest.raises(ValueError):
            mechanism.confidence_interval(100.0, confidence=1.5)
        
        with pytest.raises(ValueError):
            mechanism.confidence_interval(100.0, confidence=0.0)


class TestGaussianMechanism:
    """Test cases for GaussianMechanism."""

    def test_init(self):
        """Test mechanism initialization."""
        params = PrivacyParameters(epsilon=1.0, delta=0.01, sensitivity=1.0)
        mechanism = GaussianMechanism(params)
        assert mechanism.params == params

    def test_requires_delta(self):
        """Test that Gaussian requires delta > 0."""
        params = PrivacyParameters(epsilon=1.0, delta=0.0, sensitivity=1.0)
        mechanism = GaussianMechanism(params)
        
        with pytest.raises(DifferentialPrivacyError, match="requires delta"):
            mechanism.get_noise_scale()

    def test_add_noise_changes_value(self):
        """Test that noise is actually added."""
        params = PrivacyParameters(epsilon=1.0, delta=0.01, sensitivity=1.0)
        mechanism = GaussianMechanism(params)
        
        true_value = 100.0
        noisy_value = mechanism.add_noise(true_value)
        
        assert noisy_value != true_value

    def test_noise_scale_calculation(self):
        """Test noise scale calculation."""
        params = PrivacyParameters(epsilon=1.0, delta=0.01, sensitivity=1.0)
        mechanism = GaussianMechanism(params)
        
        scale = mechanism.get_noise_scale()
        # sigma = sensitivity * sqrt(2 * ln(1.25/delta)) / epsilon
        expected = math.sqrt(2 * math.log(1.25 / 0.01))
        assert abs(scale - expected) < 0.001

    def test_seed_reproducibility(self):
        """Test that seed produces reproducible results."""
        params = PrivacyParameters(epsilon=1.0, delta=0.01, sensitivity=1.0)
        
        mechanism1 = GaussianMechanism(params)
        mechanism1.seed(42)
        
        mechanism2 = GaussianMechanism(params)
        mechanism2.seed(42)
        
        result1 = mechanism1.add_noise(100.0)
        result2 = mechanism2.add_noise(100.0)
        
        assert result1 == result2

    def test_confidence_interval(self):
        """Test confidence interval calculation."""
        params = PrivacyParameters(epsilon=1.0, delta=0.01, sensitivity=1.0)
        mechanism = GaussianMechanism(params)
        
        value = 100.0
        lower, upper = mechanism.confidence_interval(value, confidence=0.95)
        
        assert lower < value < upper


class TestPrivacyBudgetTracker:
    """Test cases for PrivacyBudgetTracker."""

    def test_init(self):
        """Test tracker initialization."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0, total_delta=0.01)
        assert tracker.total_epsilon == 1.0
        assert tracker.total_delta == 0.01
        assert tracker.used_epsilon == 0.0
        assert tracker.used_delta == 0.0

    def test_init_basic_composition(self):
        """Test default basic composition."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0)
        assert tracker.composition == "basic"

    def test_init_invalid_epsilon(self):
        """Test error on invalid epsilon."""
        with pytest.raises(ValueError, match="total_epsilon must be positive"):
            PrivacyBudgetTracker(total_epsilon=0.0)

    def test_init_invalid_delta(self):
        """Test error on invalid delta."""
        with pytest.raises(ValueError, match="total_delta must be in \\[0, 1\"):
            PrivacyBudgetTracker(total_epsilon=1.0, total_delta=1.0)

    def test_consume_budget(self):
        """Test consuming privacy budget."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0)
        
        tracker.consume(epsilon=0.1)
        
        assert tracker.used_epsilon == 0.1
        assert tracker.used_delta == 0.0

    def test_consume_with_delta(self):
        """Test consuming budget with delta."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0, total_delta=0.01)
        
        tracker.consume(epsilon=0.1, delta=0.001)
        
        assert tracker.used_epsilon == 0.1
        assert tracker.used_delta == 0.001

    def test_consume_exceeds_epsilon_budget(self):
        """Test error when exceeding epsilon budget."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0)
        
        with pytest.raises(PrivacyBudgetExceeded):
            tracker.consume(epsilon=1.1)

    def test_consume_exceeds_delta_budget(self):
        """Test error when exceeding delta budget."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0, total_delta=0.01)
        
        tracker.consume(epsilon=0.1, delta=0.005)
        
        with pytest.raises(PrivacyBudgetExceeded):
            tracker.consume(epsilon=0.1, delta=0.006)

    def test_consume_negative_budget(self):
        """Test error on negative budget consumption."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0)
        
        with pytest.raises(ValueError, match="Cannot consume negative"):
            tracker.consume(epsilon=-0.1)

    def test_remaining_budget(self):
        """Test remaining budget calculation."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0, total_delta=0.01)
        
        tracker.consume(epsilon=0.3, delta=0.005)
        
        remaining_eps, remaining_delta = tracker.remaining()
        assert remaining_eps == 0.7
        assert remaining_delta == 0.005

    def test_get_usage_report(self):
        """Test usage report generation."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0, total_delta=0.01)
        
        tracker.consume(epsilon=0.5, delta=0.005)
        
        report = tracker.get_usage_report()
        
        assert report["total_epsilon"] == 1.0
        assert report["total_delta"] == 0.01
        assert report["used_epsilon"] == 0.5
        assert report["used_delta"] == 0.005
        assert report["remaining_epsilon"] == 0.5
        assert report["remaining_delta"] == 0.005
        assert report["epsilon_percentage_used"] == 50.0
        assert report["query_count"] == 1

    def test_multiple_consumptions(self):
        """Test multiple budget consumptions."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0)
        
        tracker.consume(epsilon=0.1)
        tracker.consume(epsilon=0.2)
        tracker.consume(epsilon=0.3)
        
        assert tracker.used_epsilon == 0.6
        assert tracker.remaining()[0] == 0.4

    def test_query_context(self):
        """Test query context manager."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0)
        
        with tracker.query_context(epsilon=0.2):
            pass
        
        assert tracker.used_epsilon == 0.2

    def test_query_context_exception(self):
        """Test that exceptions don't corrupt budget state."""
        tracker = PrivacyBudgetTracker(total_epsilon=1.0)
        
        with pytest.raises(ValueError):
            with tracker.query_context(epsilon=0.2):
                raise ValueError("Test error")
        
        # Budget was consumed even on exception
        assert tracker.used_epsilon == 0.2


class TestSensitivityCalculator:
    """Test cases for SensitivityCalculator."""

    def test_bounded_sensitivity_sum(self):
        """Test bounded sensitivity for sum query."""
        sensitivity = SensitivityCalculator.bounded_sensitivity(
            lower_bound=0.0,
            upper_bound=100.0,
            query_type="sum"
        )
        assert sensitivity == 100.0

    def test_bounded_sensitivity_count(self):
        """Test bounded sensitivity for count query."""
        sensitivity = SensitivityCalculator.bounded_sensitivity(
            lower_bound=0.0,
            upper_bound=100.0,
            query_type="count"
        )
        assert sensitivity == 1.0

    def test_bounded_sensitivity_unknown_type(self):
        """Test error on unknown query type."""
        with pytest.raises(ValueError, match="Unknown query type"):
            SensitivityCalculator.bounded_sensitivity(
                lower_bound=0.0,
                upper_bound=100.0,
                query_type="unknown"
            )


class TestDPAnonymizer:
    """Test cases for DPAnonymizer."""

    def test_init_laplace(self):
        """Test initialization with Laplace mechanism."""
        dp = DPAnonymizer(total_epsilon=1.0, mechanism="laplace")
        assert dp.mechanism_type == "laplace"
        assert dp.budget.total_epsilon == 1.0

    def test_init_gaussian(self):
        """Test initialization with Gaussian mechanism."""
        dp = DPAnonymizer(total_epsilon=1.0, total_delta=0.01, mechanism="gaussian")
        assert dp.mechanism_type == "gaussian"

    def test_init_with_seed(self):
        """Test initialization with seed."""
        dp = DPAnonymizer(total_epsilon=1.0, seed=42)
        assert dp.seed == 42

    def test_anonymize_numeric_laplace(self):
        """Test numeric anonymization with Laplace."""
        dp = DPAnonymizer(total_epsilon=1.0, mechanism="laplace", seed=42)
        
        result = dp.anonymize_numeric(value=100.0, sensitivity=1.0, epsilon=0.1)
        
        assert isinstance(result, float)
        assert result != 100.0

    def test_anonymize_numeric_gaussian(self):
        """Test numeric anonymization with Gaussian."""
        dp = DPAnonymizer(
            total_epsilon=1.0,
            total_delta=0.01,
            mechanism="gaussian",
            seed=42
        )
        
        result = dp.anonymize_numeric(value=100.0, sensitivity=1.0, epsilon=0.1)
        
        assert isinstance(result, float)
        assert result != 100.0

    def test_anonymize_count(self):
        """Test count anonymization."""
        dp = DPAnonymizer(total_epsilon=1.0, seed=42)
        
        result = dp.anonymize_count(count=100, epsilon=0.1)
        
        assert isinstance(result, float)

    def test_anonymize_sum(self):
        """Test sum anonymization."""
        dp = DPAnonymizer(total_epsilon=1.0, seed=42)
        
        result = dp.anonymize_sum(
            sum_value=1000.0,
            lower_bound=0.0,
            upper_bound=100.0,
            epsilon=0.1
        )
        
        assert isinstance(result, float)

    def test_budget_tracking(self):
        """Test that budget is properly tracked."""
        dp = DPAnonymizer(total_epsilon=1.0)
        
        initial_remaining = dp.budget.remaining()[0]
        
        dp.anonymize_numeric(value=100.0, sensitivity=1.0, epsilon=0.1)
        
        remaining = dp.budget.remaining()[0]
        assert remaining < initial_remaining

    def test_get_budget_report(self):
        """Test budget report."""
        dp = DPAnonymizer(total_epsilon=1.0, total_delta=0.01)
        
        dp.anonymize_numeric(value=100.0, sensitivity=1.0, epsilon=0.1)
        
        report = dp.get_budget_report()
        
        assert "total_epsilon" in report
        assert "used_epsilon" in report
        assert report["used_epsilon"] > 0

    def test_unknown_mechanism(self):
        """Test error on unknown mechanism."""
        dp = DPAnonymizer(total_epsilon=1.0, mechanism="unknown")
        
        with pytest.raises(ValueError, match="Unknown mechanism"):
            dp.anonymize_numeric(value=100.0, sensitivity=1.0)
