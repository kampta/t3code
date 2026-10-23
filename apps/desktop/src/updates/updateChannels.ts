// Preview builds share nightly branding but ship without an update feed.
const PRERELEASE_VERSION_PATTERN = /^[^-+]+-(?:nightly|preview)\.\d{8}\.\d+$/;
const DEVELOPMENT_VERSION_PATTERN = /-dev(?:\.|$)/;

export function isDevelopmentDesktopVersion(version: string): boolean {
  return DEVELOPMENT_VERSION_PATTERN.test(version);
}

export function isNightlyDesktopVersion(version: string): boolean {
  return PRERELEASE_VERSION_PATTERN.test(version);
}
