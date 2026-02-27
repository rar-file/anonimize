# Security Policy

## Supported Versions

The following versions of Anonimize are currently supported with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| 0.1.x   | :x:                |
| < 0.1   | :x:                |

## Reporting a Vulnerability

We take security seriously. If you discover a security vulnerability within Anonimize, please follow these steps:

### 1. Do Not Disclose Publicly

Please **DO NOT** create a public GitHub issue for security vulnerabilities.

### 2. Contact Us Directly

Send an email to security@anonimize.dev with:

- **Subject**: "Security Vulnerability in Anonimize"
- **Description**: A detailed description of the vulnerability
- **Impact**: What could an attacker accomplish?
- **Reproduction**: Steps to reproduce the issue
- **Environment**: Version, Python version, OS, etc.
- **Possible fix**: If you have suggestions for fixing the issue

### 3. What to Expect

- **Initial Response**: Within 48 hours, we will acknowledge receipt of your report
- **Investigation**: We will investigate and validate the vulnerability
- **Updates**: We will provide updates on our progress every 5 business days
- **Resolution**: Once fixed, we will release a security patch and credit you (if desired)

### 4. Disclosure Timeline

We follow a coordinated disclosure process:

1. **Day 0**: Vulnerability reported
2. **Day 2**: Acknowledgment sent
3. **Day 30**: Target date for fix (may vary by severity)
4. **Day 45**: Public disclosure after fix is released

We may adjust this timeline based on severity and complexity.

## Security Best Practices

### For Users

1. **Keep Updated**: Always use the latest version of Anonimize
2. **Validate Inputs**: Validate all data before anonymization
3. **Secure Storage**: Store anonymized data securely
4. **Access Control**: Limit access to anonymization tools
5. **Audit Logs**: Maintain logs of anonymization operations

### For Developers

1. **Dependency Management**: Keep all dependencies updated
2. **Code Review**: All security-related code must be reviewed
3. **Testing**: Include security tests in your test suite
4. **Documentation**: Document security considerations
5. **Least Privilege**: Use minimal necessary permissions

## Security Features

Anonimize includes several security-focused features:

### Data Protection

- **Deterministic Hashing**: Option for consistent anonymization
- **Salt Support**: Configurable salt for hash operations
- **Relationship Preservation**: Maintains data integrity
- **Audit Trail**: Optional logging of anonymization operations

### Best Practices Built-in

- No data exfiltration
- Local processing (no cloud dependencies)
- Configurable logging levels
- Memory-efficient processing for large datasets

## Known Security Considerations

### Hash Collisions

While we use standard hashing algorithms (SHA-256 by default), be aware that:

- Hash collisions are theoretically possible
- Consider the sensitivity of your data
- Use salts for additional security

### Reversibility

- **Replace strategy**: Not reversible (original data is lost)
- **Hash strategy**: One-way only
- **Mask strategy**: Partial data remains (consider exposure risk)

### Performance vs Security

Some performance optimizations may have security implications:

- Caching: May retain data in memory longer
- Batch processing: Larger exposure window
- Streaming: Minimal memory footprint (recommended for sensitive data)

## Security Testing

Our security testing includes:

- Static analysis with Bandit
- Dependency vulnerability scanning
- Regular security audits
- Fuzzing tests for input validation

## Compliance Notes

Anonimize can assist with compliance efforts but does not guarantee compliance:

- **GDPR**: Helps with data minimization and pseudonymization
- **CCPA**: Supports data de-identification
- **HIPAA**: Can be part of a de-identification strategy

Consult with legal counsel for compliance requirements.

## Credits

We thank the following security researchers who have responsibly disclosed vulnerabilities:

- [Your name could be here!]

## Contact

- **Security Team**: security@anonimize.dev
- **General Inquiries**: contact@anonimize.dev
- **PGP Key**: [Available upon request]

---

Last Updated: 2024
