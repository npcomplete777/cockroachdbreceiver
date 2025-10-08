### 2c. Create SECURITY.md
Create `~/cockroachdbreceiver/cockroachreceiver/SECURITY.md`:
```markdown
# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

Please report security vulnerabilities to: [your-email@domain.com]

Do NOT create public GitHub issues for security vulnerabilities.

## Security Best Practices

1. **Never store credentials in code**
   - Use environment variables or secret management systems
   - Never commit connection strings with passwords

2. **Use minimal database permissions**
   - Grant only SELECT on required crdb_internal tables
   - Use a dedicated monitoring user

3. **Enable TLS/SSL**
   - Always use sslmode=require or sslmode=verify-full in production
   - Validate certificates when possible

4. **Limit query permissions**
   - The receiver only needs read access
   - Never grant write permissions to the monitoring user
