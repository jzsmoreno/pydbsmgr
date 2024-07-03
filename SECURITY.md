# Security Policy

- [Security Response Team](#security-response-team)
- [Supported Versions](#supported-versions)
- [Reporting a Vulnerability](#reporting-a-vulnerability)
- [Acknowledgments](#acknowledgments)
- [Disclosure Policy](#disclosure-policy)
- [CVSS v3.0](#cvss-v30-summary)

## Security Response Team

Our security response team is available to handle security issues. You can contact us at [jzs.gm27@gmail.com](mailto:jzs.gm27@gmail.com).

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the severity of the vulnerability:

| CVSS v3.0 | Supported Versions                        |
| --------- | ----------------------------------------- |
| 9.0-10.0  | Releases within the previous three months |
| 4.0-8.9   | Most recent release                       |
| < 4.0     | 0.0.3 release                             |

The following versions of Python packages are currently supported:

- numpy: >=1.19.0, <2.0.0
- pandas: >=1.5.3
- azure-storage-blob: >=12.16.0
- python-dotenv: >=1.0.0
- openpyxl: >=3.1.2
- pyarrow: >=15.0.0
- fastparquet: >=2024.2.0

## Reporting a Vulnerability

Please report any suspected security vulnerabilities to **[jzs.gm27@gmail.com](jzs.gm27@gmail.com)**. You will receive a response from us within 48 hours. If the issue is confirmed, we will release a patch as soon as possible, typically within a few days depending on complexity.

We appreciate your help in keeping our project secure.

## Acknowledgments

We acknowledge and credit individuals who report security vulnerabilities responsibly. If you report a security issue, we will include your name or alias in our acknowledgments, unless you request to remain anonymous.

## Disclosure Policy

We follow a responsible disclosure policy. After receiving a report, we will work with the reporter to address the issue and disclose it publicly once a fix is available. We may also coordinate with package maintainers if necessary.

## CVSS v3.0 Summary

The Common Vulnerability Scoring System (CVSS) v3.0 is an open standard for assessing the severity of security vulnerabilities. It provides a numerical score from 0.0 to 10.0, with higher scores indicating more severe vulnerabilities. Organizations use CVSS scores to prioritize their response to security vulnerabilities based on their severity.