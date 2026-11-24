# Security

telelem is an engineering experiment. It does not yet offer a supported production release. Use synthetic payloads and short-lived local fixture credentials; do not expose the test gateway or fixture controls to an untrusted network.

## Report a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/dettmar/telelem/security/advisories/new) if available. If GitHub does not offer that form, contact the maintainer privately through the repository owner's listed contact channel. Do not post sensitive details in a public issue.

Include the affected commit or version, the observed impact, and the smallest safe local reproduction. Redact credentials, generated keys, request payloads and identifying information. Do not test against someone else's deployment. Please coordinate public disclosure with the maintainer.

## Scope

CI checks reduce risk; they are not a security audit. The release process documents which checks run, what they cover, and which GitHub controls require repository settings.
