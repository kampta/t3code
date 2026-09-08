import { useAtomValue } from "@effect/atom-react";
import {
  REACTION_IMAGE,
  resolveSessionReaction,
} from "@t3tools/client-runtime/state/session-activity";
import { Image } from "expo-image";
import { memo } from "react";
import { View } from "react-native";

import { activeSessionCountAtom } from "../state/sessionActivity";
import { AppText as Text } from "./AppText";

const REACTION_SOURCE = require("../../../../assets/memes/mcmahon-reaction.png");
const THUMBNAIL_WIDTH = 90;
const IMAGE_SCALE = THUMBNAIL_WIDTH / REACTION_IMAGE.cropWidth;

export const SessionReaction = memo(function SessionReaction() {
  const activeCount = useAtomValue(activeSessionCountAtom);
  const reaction = resolveSessionReaction(activeCount);
  if (reaction === null) {
    return null;
  }

  return (
    <View
      accessible
      accessibilityLabel={`${activeCount} active sessions. ${reaction.label}`}
      className="mx-3 my-2 flex-row items-center gap-3 rounded-xl bg-subtle p-2"
    >
      <View
        accessible={false}
        accessibilityElementsHidden
        importantForAccessibility="no-hide-descendants"
        className="overflow-hidden rounded-lg"
        style={{
          width: THUMBNAIL_WIDTH,
          height: REACTION_IMAGE.cropHeight * IMAGE_SCALE,
        }}
      >
        <Image
          source={REACTION_SOURCE}
          accessible={false}
          contentFit="fill"
          transition={0}
          style={{
            position: "absolute",
            width: REACTION_IMAGE.width * IMAGE_SCALE,
            height: REACTION_IMAGE.height * IMAGE_SCALE,
            left: -REACTION_IMAGE.cropLeft * IMAGE_SCALE,
            top: -reaction.cropTop * IMAGE_SCALE,
          }}
        />
      </View>
      <View className="flex-1 gap-1">
        <Text className="text-sm font-t3-medium">{activeCount} active sessions</Text>
        <Text className="text-xs text-foreground-muted">{reaction.label}</Text>
      </View>
    </View>
  );
});
