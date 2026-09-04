const { withEntitlementsPlist } = require("expo/config-plugins");

function stripUnsupportedEntitlements(entitlements) {
  delete entitlements["aps-environment"];
  delete entitlements["com.apple.developer.applesignin"];
  delete entitlements["com.apple.developer.associated-domains"];
  delete entitlements["com.apple.security.application-groups"];
  return entitlements;
}

module.exports = function withoutIosPersonalTeamCapabilities(config) {
  return withEntitlementsPlist(config, (modConfig) => {
    stripUnsupportedEntitlements(modConfig.modResults);
    return modConfig;
  });
};

module.exports.stripUnsupportedEntitlements = stripUnsupportedEntitlements;
