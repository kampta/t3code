# Product usage data

The T3 Code server can send product usage events to PostHog, associated with a hashed account or
installation identifier. Collection is disabled by default. Events include the provider, model,
reasoning effort, permission mode, turn result, duration, and main-agent token totals when
available.

Events do not include prompts, responses, file contents, authentication tokens, conversation IDs,
raw provider events, or child-agent output. Child-agent token use is excluded from the totals.

To opt in, set `T3CODE_TELEMETRY_ENABLED=true` in the server's environment before starting it.
Remove the variable or set it to `false` to stop recording and sending product events again.
