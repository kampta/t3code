import type { DesktopUpdateChannel } from "@t3tools/contracts";

const NIGHTLY_VERSION_PATTERN = /-nightly\.\d{8}\.\d+$/;
const DEVELOPMENT_VERSION_PATTERN = /-dev(?:\.|$)/;

export function isDevelopmentDesktopVersion(version: string): boolean {
  return DEVELOPMENT_VERSION_PATTERN.test(version);
}

export function isNightlyDesktopVersion(version: string): boolean {
  return NIGHTLY_VERSION_PATTERN.test(version);
}

export function resolveDefaultDesktopUpdateChannel(appVersion: string): DesktopUpdateChannel {
  return isNightlyDesktopVersion(appVersion) ? "nightly" : "latest";
}
