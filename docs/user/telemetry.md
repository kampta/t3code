# Telemetry

This build disables product analytics and agent feedback uploads. It also disables
relay tracing, browser trace uploads, mobile crash report uploads, and
OpenTelemetry export to remote collectors. Local logs, traces, and resource diagnostics remain available.

The server does not publish agent activity for mobile notifications. Pairing, T3
Connect, and remote access still work. Hosted clients and existing mobile
installs use the telemetry settings in their own builds.
